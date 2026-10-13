/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package epp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
	"github.com/llm-d/llm-d-router/test/integration"
)

// A wix-shaped profile: health gate -> impala-admission-filter -> session filter -> scorers.
const impalaAdmissionConfig = `
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: openai-parser
  - type: mock-metrics-source
  - type: utilization-filter
    name: session-health-gate
    parameters:
      conditions:
        - metric: waiting-queue
          maxValue: 8
      fallbackOnEmpty: true
  - type: session-affinity-filter
    parameters:
      strategy: session_id
      sessionIdConfig:
        sources:
          - header: x-session-id
        evictionTtlSeconds: 3600
  - type: impala-admission-filter
    parameters:
      sessionAffinityPluginName: session-affinity-filter
      burstMinutes: 1
  - type: prefix-cache-scorer
  - type: max-score-picker
requestHandler:
  parsers:
  - pluginRef: openai-parser
dataLayer:
  sources:
  - pluginRef: mock-metrics-source
schedulingProfiles:
  - name: default
    plugins:
    - pluginRef: session-health-gate
    - pluginRef: impala-admission-filter
    - pluginRef: session-affinity-filter
    - pluginRef: prefix-cache-scorer
      weight: 1
    - pluginRef: max-score-picker
`

// A prefix-only plane (no sessionAffinityPluginName): the only signal the
// filter has is the approximate prefix index.
const impalaPrefixOnlyConfig = `
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: openai-parser
  - type: mock-metrics-source
  - type: impala-admission-filter
  - type: prefix-cache-scorer
  - type: max-score-picker
requestHandler:
  parsers:
  - pluginRef: openai-parser
dataLayer:
  sources:
  - pluginRef: mock-metrics-source
schedulingProfiles:
  - name: default
    plugins:
    - pluginRef: impala-admission-filter
    - pluginRef: prefix-cache-scorer
      weight: 1
    - pluginRef: max-score-picker
`

// approxPrefixProducerName is the name the auto-created default
// approx-prefix-cache-producer carries (named by its type), which is where the
// filter's hard-coded PrefixCacheMatchInfo key points.
const approxPrefixProducerName = "approx-prefix-cache-producer"

var impalaRequestSeq int64

// endpointID is the datastore's endpoint ID.Name of k8s-style harness pod
// <index>, which is what every impala_* metric carries as its `endpoint` label.
// impalaLabelPrefix is the namespace of the lifecycle labels these tests drive.
const impalaLabelPrefix = "impala.ai/"

// hasImpalaLabel reports whether any impala.ai/* lifecycle label is still
// visible; it is the "settled" predicate for clearing a pod's labels.
func hasImpalaLabel(labels map[string]string) bool {
	for k := range labels {
		if strings.HasPrefix(k, impalaLabelPrefix) {
			return true
		}
	}
	return false
}

func endpointID(index int) string { return fmt.Sprintf("pod-%d-rank-0", index) }

// endpointAddr is the address the harness gives pod <index>.
func endpointAddr(index int) string { return fmt.Sprintf("192.168.1.%d:8000", index+1) }

// indexOfEndpoint maps a picked destination address back to its pod index.
func indexOfEndpoint(t *testing.T, addr string, pods int) int {
	t.Helper()
	for i := range pods {
		if addr == endpointAddr(i) {
			return i
		}
	}
	require.Failf(t, "unknown endpoint", "picked %q is none of the %d pods", addr, pods)
	return -1
}

// uniquePrompt is a short prompt whose first block no other request shares, so
// the approximate prefix index never ties two requests together (a shared
// prompt would teach the serving endpoint a block every later request matches).
func uniquePrompt() string {
	return fmt.Sprintf("unique request %d, no shared prefix", atomic.AddInt64(&impalaRequestSeq, 1))
}

// conversationPrompt builds turn-<turn> of conversation <tag>: a 1024-byte
// history (exactly four 64-token blocks under the estimate tokenizer's 4
// bytes/token) followed by a per-turn suffix. Every turn of one conversation
// therefore matches the same four blocks; conversations with different tags
// share nothing.
func conversationPrompt(tag string, turn int) string {
	unit := fmt.Sprintf("[%s] ", tag)
	unit += strings.Repeat("x", 64-len(unit))
	return strings.Repeat(unit, 16) + fmt.Sprintf(" turn %d: and then what happened next?", turn)
}

// routePrompt sends one /v1/completions request (with x-session-id when
// sessionID != "") and completes it with the given upstream status, returning
// the endpoint the EPP picked.
func routePrompt(t *testing.T, h *TestHarness, sessionID, prompt, status string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := extProcPb.NewExternalProcessorClient(h.grpcConn).Process(ctx)
	require.NoError(t, err)
	reqHeaders := map[string]string{
		metadata.ObjectiveKey:        modelMyModel,
		metadata.ModelNameRewriteKey: modelMyModelTarget,
		reqcommon.RequestIDHeaderKey: fmt.Sprintf("impala-req-%d", atomic.AddInt64(&impalaRequestSeq, 1)),
	}
	if sessionID != "" {
		reqHeaders["x-session-id"] = sessionID
	}
	body, err := json.Marshal(map[string]any{"model": modelMyModel, "prompt": prompt, "max_tokens": 10, "temperature": 0})
	require.NoError(t, err)
	for _, req := range integration.ReqRaw(reqHeaders, string(body)) {
		require.NoError(t, client.Send(req))
	}
	headersResp, err := client.Recv()
	require.NoError(t, err)
	endpoint := headerValue(headersResp.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders(), metadata.DestinationEndpointKey)
	require.NotEmpty(t, endpoint)
	_, err = client.Recv()
	require.NoError(t, err)
	respBody := `{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"hi","role":"assistant"}}],"model":"` +
		modelMyModelTarget + `","object":"chat.completion","usage":{"completion_tokens":2,"prompt_tokens":3,"total_tokens":5}}`
	if status != "200" {
		respBody = `{"error":{"message":"upstream unavailable"}}`
	}
	for _, req := range ReqResponseOnly(map[string]string{"content-type": "application/json", ":status": status}, respBody) {
		require.NoError(t, client.Send(req))
	}
	for range 2 {
		_, err = client.Recv()
		require.NoError(t, err)
	}
	require.NoError(t, client.CloseSend())
	return endpoint
}

// routeSession sends one request on session sessionID with a unique prompt.
func routeSession(t *testing.T, h *TestHarness, sessionID, status string) string {
	t.Helper()
	return routePrompt(t, h, sessionID, uniquePrompt(), status)
}

// impalaMetric sums the series of family `name` whose labels include `labels`
// (counters and gauges alike); 0 when there is none. The collectors are
// package-level and outlive a harness, so counter assertions use deltas.
func impalaMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	total := 0.0
	for _, v := range impalaSeries(t, name, labels) {
		total += v
	}
	return total
}

// impalaSeries returns the value of every series of family `name` whose labels
// include `labels`, so a caller can tell an absent series from a zero one.
func impalaSeries(t *testing.T, name string, labels map[string]string) []float64 {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	require.NoError(t, err)
	var values []float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	series:
		for _, m := range family.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			for k, want := range labels {
				if have[k] != want {
					continue series
				}
			}
			if m.GetCounter() != nil {
				values = append(values, m.GetCounter().GetValue())
			} else {
				values = append(values, m.GetGauge().GetValue())
			}
		}
	}
	return values
}

// approxIndexBlocks reads the auto-created approx-prefix-cache-producer's
// per-endpoint block counts, keyed by endpoint ID.Name.
func approxIndexBlocks(t *testing.T, h *TestHarness) map[string]int {
	t.Helper()
	dumper, ok := h.Runner.PluginHandle.Plugin(approxPrefixProducerName).(fwkplugin.StateDumper)
	require.True(t, ok, "the %q plugin must exist (auto-created for impala-admission-filter's PrefixCacheMatchInfo) and dump state", approxPrefixProducerName)
	raw, err := dumper.DumpState()
	require.NoError(t, err)
	var state struct {
		Pods []struct {
			Pod    string `json:"pod"`
			Blocks int    `json:"blocks"`
		} `json:"pods"`
	}
	require.NoError(t, json.Unmarshal(raw, &state))
	blocks := map[string]int{}
	for _, p := range state.Pods {
		blocks[p.Pod[strings.LastIndex(p.Pod, "/")+1:]] = p.Blocks
	}
	return blocks
}

// TestImpalaAdmission_RetainKeepsBoundSessionsAndSteersNewOnes proves, through
// the real ext_proc path: the plugin loads with a sessionAffinityPluginName
// (loader DAG), a label-only pod update reaches it without resetting the
// session binding (spec G1, EPP side), a bound session stays on the draining
// endpoint, new sessions never land there, and the contract metrics appear on
// the EPP registry.
func TestImpalaAdmission_RetainKeepsBoundSessionsAndSteersNewOnes(t *testing.T) {
	pods := []PodState{P(0, 0, 0, modelMyModelTarget), P(1, 0, 0, modelMyModelTarget), P(2, 0, 0, modelMyModelTarget)}
	h := NewTestHarness(t.Context(), t, WithStandardMode(), WithConfigText(impalaAdmissionConfig)).WithBaseResources()
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)

	// bind s-bound somewhere, then drain that pod
	home := routeSession(t, h, "s-bound", "200")
	homeIdx := indexOfEndpoint(t, home, len(pods))
	homeName := endpointID(homeIdx)
	retained := map[string]string{"endpoint": homeName, "decision": "retained"}
	steered := map[string]string{"endpoint": homeName, "decision": "steered"}
	retainedBefore := impalaMetric(t, "impala_admission_decisions_total", retained)
	steeredBefore := impalaMetric(t, "impala_admission_decisions_total", steered)
	labelErrorsBefore := impalaMetric(t, "impala_admission_label_errors_total", nil)
	panicsBefore := impalaMetric(t, "impala_admission_panics_total", nil)

	h.SetPodLabels(homeIdx, map[string]string{"impala.ai/state": "draining", "impala.ai/phase": "retain"}, nil)

	for range 5 {
		assert.Equal(t, home, routeSession(t, h, "s-bound", "200"), "a bound session stays on its draining endpoint")
	}
	for i := range 30 {
		assert.NotEqual(t, home, routeSession(t, h, fmt.Sprintf("s-new-%d", i), "200"), "a new session never lands on a draining endpoint")
	}
	assert.Equal(t, 5.0, impalaMetric(t, "impala_admission_decisions_total", retained)-retainedBefore)
	assert.Equal(t, 30.0, impalaMetric(t, "impala_admission_decisions_total", steered)-steeredBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_admission_label_errors_total", nil)-labelErrorsBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_admission_panics_total", nil)-panicsBefore)

	// errors per endpoint: two 502s served by the home endpoint
	errorsLabels := map[string]string{"endpoint": homeName, "code": "502"}
	errorsBefore := impalaMetric(t, "impala_endpoint_errors_total", errorsLabels)
	routeSession(t, h, "s-bound", "502")
	routeSession(t, h, "s-bound", "502")
	assert.Equal(t, 2.0, impalaMetric(t, "impala_endpoint_errors_total", errorsLabels)-errorsBefore)

	// The periodic refresh publishes the drained endpoint's bindings and load. s-bound
	// is the only session bound there (new sessions are steered away), so both are
	// exactly 1; the series also being absent for a pod that left the pool is
	// covered by the plugin's unit tests.
	require.Eventually(t, func() bool {
		return impalaMetric(t, "impala_session_bindings", map[string]string{"endpoint": homeName}) == 1 &&
			impalaMetric(t, "impala_admission_endpoint_load", map[string]string{"endpoint": homeName}) == 1
	}, 25*time.Second, 500*time.Millisecond, "refresh never published bindings=1, load=1 for %s", homeName)

	// label removed -> active again; the plugin is a pass-through and the session is still bound
	h.SetPodLabels(homeIdx, nil, func(observed map[string]string) bool { return !hasImpalaLabel(observed) })
	assert.Equal(t, home, routeSession(t, h, "s-bound", "200"))
}

// TestImpalaAdmission_WarmingTakesNewSessions proves share=1 forces every
// placeable request onto the warming endpoint, and that the overload stop (fed
// by the refresh loop's recent-binding counts) then hands requests back to the
// scorers.
func TestImpalaAdmission_WarmingTakesNewSessions(t *testing.T) {
	pods := []PodState{
		P(0, 0, 0, modelMyModelTarget),
		P(1, 0, 0, modelMyModelTarget),
		P(2, 0, 0, modelMyModelTarget).WithLabels(map[string]string{"impala.ai/state": "warming", "impala.ai/share": "1"}),
	}
	h := NewTestHarness(t.Context(), t, WithStandardMode(), WithConfigText(impalaAdmissionConfig)).WithBaseResources()
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)

	warmingName := endpointID(2)
	forced := map[string]string{"endpoint": warmingName, "decision": "forced"}
	forcedBefore := impalaMetric(t, "impala_admission_decisions_total", forced)

	// The plugin refreshes as soon as the datastore lists the pool (about 1 s at
	// most, not the 15 s interval), publishing a zero load series for every
	// endpoint, so an EPP restart never leaves the series absent. Waiting for that
	// refresh also keeps it out of the burst below. A stale series from an earlier
	// harness cannot stand in: the retain test leaves a load of 1 behind. Until the
	// next refresh, 15 s later, the recent-binding cache stays empty, so
	// load(W) = 0 < mean+1 and every placeable request is forced onto W.
	require.Eventually(t, func() bool {
		loads := impalaSeries(t, "impala_admission_endpoint_load", nil)
		return len(loads) == len(pods) && impalaMetric(t, "impala_admission_endpoint_load", nil) == 0 &&
			len(impalaSeries(t, "impala_session_bindings", nil)) == len(pods)
	}, 5*time.Second, 50*time.Millisecond, "the startup refresh never published a zero load series for every endpoint")
	const forcedRequests = 6
	for i := range forcedRequests {
		assert.Equal(t, endpointAddr(2), routeSession(t, h, fmt.Sprintf("w-%d", i), "200"), "share=1 forces the placeable request onto the warming endpoint")
	}
	assert.Equal(t, float64(forcedRequests), impalaMetric(t, "impala_admission_decisions_total", forced)-forcedBefore)

	// After the refresh W's recent bindings (>= forcedRequests) exceed the settled
	// mean (0) + 1, so the overload stop engages: no further decision is "forced".
	require.Eventually(t, func() bool {
		return impalaMetric(t, "impala_admission_endpoint_load", map[string]string{"endpoint": warmingName}) >= forcedRequests
	}, 25*time.Second, 500*time.Millisecond, "refresh never published the warming endpoint's recent bindings")
	forcedBefore = impalaMetric(t, "impala_admission_decisions_total", forced)
	for i := range 10 {
		routeSession(t, h, fmt.Sprintf("w-after-%d", i), "200")
	}
	assert.Equal(t, 0.0, impalaMetric(t, "impala_admission_decisions_total", forced)-forcedBefore, "the overload stop must end forcing once W holds more than the mean")
}

// TestImpalaAdmission_PrefixOnlyRetainsHistoryAndSteersFreshConversations is the
// proof that PrefixCacheMatchInfo reaches the filter through the auto-created
// approx-prefix-cache-producer. A draining endpoint is retained ONLY because it
// really holds the conversation's history in the approximate index, and is
// steered for a conversation it has never seen. If the filter read a match of 0
// the retained requests below would be steered away and the test would fail.
func TestImpalaAdmission_PrefixOnlyRetainsHistoryAndSteersFreshConversations(t *testing.T) {
	pods := []PodState{P(0, 0, 0, modelMyModelTarget), P(1, 0, 0, modelMyModelTarget), P(2, 0, 0, modelMyModelTarget)}
	h := NewTestHarness(t.Context(), t, WithStandardMode(), WithConfigText(impalaPrefixOnlyConfig)).WithBaseResources()
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)

	// Build the history: one turn, so exactly one endpoint learns it. Wait until the
	// index shows it there (the add is asynchronous) before sending anything else,
	// otherwise a second request could teach a second endpoint the same history.
	home := routePrompt(t, h, "", conversationPrompt("alpha", 0), "200")
	homeIdx := indexOfEndpoint(t, home, len(pods))
	homeName := endpointID(homeIdx)
	require.Eventually(t, func() bool {
		return approxIndexBlocks(t, h)[homeName] >= 4
	}, 5*time.Second, 20*time.Millisecond, "the approximate index never learned the history on %s", homeName)
	require.Len(t, approxIndexBlocks(t, h), 1, "only the endpoint that served the history may hold it")

	// Still active: the prefix scorer alone sends the next turn to the holder.
	assert.Equal(t, home, routePrompt(t, h, "", conversationPrompt("alpha", 1), "200"))

	retained := map[string]string{"endpoint": homeName, "decision": "retained"}
	steered := map[string]string{"endpoint": homeName, "decision": "steered"}
	retainedBefore := impalaMetric(t, "impala_admission_decisions_total", retained)
	steeredBefore := impalaMetric(t, "impala_admission_decisions_total", steered)

	h.SetPodLabels(homeIdx, map[string]string{"impala.ai/state": "draining", "impala.ai/phase": "retain"}, nil)

	// Later turns of the same conversation hold a four-block advantage on the
	// draining endpoint: retained.
	for turn := 2; turn < 7; turn++ {
		assert.Equal(t, home, routePrompt(t, h, "", conversationPrompt("alpha", turn), "200"),
			"a conversation whose history the draining endpoint holds stays on it")
	}
	assert.Equal(t, 5.0, impalaMetric(t, "impala_admission_decisions_total", retained)-retainedBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_admission_decisions_total", steered)-steeredBefore)

	// Fresh conversations have no advantage there: steered, never placed on it.
	for i := range 20 {
		assert.NotEqual(t, home, routePrompt(t, h, "", conversationPrompt(fmt.Sprintf("fresh-%d", i), 0), "200"),
			"a fresh conversation never lands on a draining endpoint")
	}
	assert.Equal(t, 20.0, impalaMetric(t, "impala_admission_decisions_total", steered)-steeredBefore)
	assert.Equal(t, 5.0, impalaMetric(t, "impala_admission_decisions_total", retained)-retainedBefore)
}
