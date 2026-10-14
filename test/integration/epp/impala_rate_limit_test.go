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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A prefix-only plane with the rate-limit filter. The harness feeds metrics through its mock
// source (fwkdl.Metrics, no Prometheus payload), so the plugin's extractor role never sees a
// counter: this test proves the config loads with the plugin, that a config whose own sources
// lack a usable metrics payload still routes (IfMissing: Warn), and that unmeasured limits
// never drop an endpoint (fail open). The drop path is unit-tested on real exposition text.
const impalaRateLimitConfig = `
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
  - type: openai-parser
  - type: mock-metrics-source
  - type: impala-rate-limit-filter
    parameters:
      windowSeconds: 10
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
    - pluginRef: impala-rate-limit-filter
    - pluginRef: prefix-cache-scorer
      weight: 1
    - pluginRef: max-score-picker
`

func TestImpalaRateLimit_LoadsAndFailsOpenWithoutMeasurements(t *testing.T) {
	pods := []PodState{P(0, 0, 0, modelMyModelTarget), P(1, 0, 0, modelMyModelTarget), P(2, 0, 0, modelMyModelTarget)}
	h := NewTestHarness(t.Context(), t, WithStandardMode(), WithConfigText(impalaRateLimitConfig)).WithBaseResources()
	h.WithPods(pods).WaitForSync(len(pods), modelMyModel)

	// The tightest possible limits on pod 0: with no measurement they must change nothing.
	h.SetPodLabels(0, map[string]string{"impala.ai/limit-rpm": "1", "impala.ai/limit-tpm": "1"},
		func(observed map[string]string) bool { return observed["impala.ai/limit-rpm"] == "1" })

	droppedBefore := impalaMetric(t, "impala_rate_limit_dropped_total", nil)
	failOpenBefore := impalaMetric(t, "impala_rate_limit_fail_open_total", nil)
	labelErrorsBefore := impalaMetric(t, "impala_rate_limit_label_errors_total", nil)
	panicsBefore := impalaMetric(t, "impala_rate_limit_panics_total", nil)

	seen := map[int]int{}
	for range 30 {
		seen[indexOfEndpoint(t, routePrompt(t, h, "", uniquePrompt(), "200"), len(pods))]++
	}
	assert.Positive(t, seen[0], "a limited but unmeasured endpoint keeps taking traffic: %v", seen)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_rate_limit_dropped_total", nil)-droppedBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_rate_limit_fail_open_total", nil)-failOpenBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_rate_limit_label_errors_total", nil)-labelErrorsBefore)
	assert.Equal(t, 0.0, impalaMetric(t, "impala_rate_limit_panics_total", nil)-panicsBefore)

	// The publish loop (every 5 s) reports pod 0's limited dimensions as unmeasured.
	require.Eventually(t, func() bool {
		return impalaMetric(t, "impala_rate_limit_unmeasured", map[string]string{"endpoint": endpointID(0), "dimension": "rpm"}) == 1 &&
			impalaMetric(t, "impala_rate_limit_unmeasured", map[string]string{"endpoint": endpointID(0), "dimension": "tpm"}) == 1
	}, 15*time.Second, 250*time.Millisecond)
	assert.Empty(t, impalaSeries(t, "impala_rate_limit_engine_rate", map[string]string{"endpoint": endpointID(0)}),
		"no rate is published without a measurement")
}
