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

package impalaadmission

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload"
	sessionaffinityfilter "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/sessionaffinity"
	sessionutil "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/util/sessionaffinity"
	"github.com/llm-d/llm-d-router/test/utils"
)

const testBlock = 256

// resetMetrics clears every vector collector between tests. Plain Counters
// cannot be reset; tests read deltas for labelErrorsTotal and panicsTotal.
func resetMetrics() {
	decisionsTotal.Reset()
	movedTokensTotal.Reset()
	bucketTokens.Reset()
	endpointLoad.Reset()
	sessionBindings.Reset()
	sessionBindingsRecent.Reset()
	endpointErrorsTotal.Reset()
}

// testHandle returns a plugin handle whose context is cancelled when the test
// ends, so the refresh loop (and the session sweeper) started by a factory do
// not outlive it.
func testHandle(t *testing.T) fwkplugin.Handle {
	t.Helper()
	ctx, cancel := context.WithCancel(utils.NewTestContext(t))
	t.Cleanup(cancel)
	return utils.NewTestHandle(ctx)
}

func testEndpoint(name string, labels map[string]string, matchBlocks int) fwksched.Endpoint {
	ep := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: name}, Name: name, Labels: labels},
		&fwkdl.Metrics{}, nil)
	ep.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(matchBlocks, 20, testBlock))
	return ep
}

func withInFlight(ep fwksched.Endpoint, requests int64) fwksched.Endpoint {
	ep.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Requests: requests})
	return ep
}

func names(eps []fwksched.Endpoint) []string {
	out := make([]string, len(eps))
	for i, e := range eps {
		out[i] = e.GetMetadata().ID.Name
	}
	return out
}

func ids(names ...string) []k8stypes.NamespacedName {
	out := make([]k8stypes.NamespacedName, len(names))
	for i, n := range names {
		out[i] = k8stypes.NamespacedName{Namespace: "default", Name: n}
	}
	return out
}

// newTestPlugin builds the plugin with a fixed clock and a zero RNG. When
// withSession is true a session-affinity-filter (strategy session_id) named
// "sa" is registered on the handle first and returned.
func newTestPlugin(t *testing.T, params string, withSession bool) (*Plugin, *sessionaffinityfilter.SessionAffinity) {
	t.Helper()
	resetMetrics()
	handle := testHandle(t)
	var sa *sessionaffinityfilter.SessionAffinity
	if withSession {
		plg, err := sessionaffinityfilter.Factory("sa", json.NewDecoder(strings.NewReader(
			`{"strategy":"session_id","sessionIdConfig":{"sources":[{"header":"x-session-id"}],"evictionTtlSeconds":3600}}`)), handle)
		require.NoError(t, err)
		sa = plg.(*sessionaffinityfilter.SessionAffinity)
		handle.AddPlugin("sa", sa)
	}
	plg, err := Factory("adm", fwkplugin.StrictDecoder(json.RawMessage(params)), handle)
	require.NoError(t, err)
	p := plg.(*Plugin)
	p.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	p.random = func() float64 { return 0 }
	return p, sa
}

// bind records a session binding the way the live session-affinity-filter
// does (PreRequest after a pick); the binding is stamped with the wall clock.
func bind(t *testing.T, sa *sessionaffinityfilter.SessionAffinity, sessionID string, ep fwksched.Endpoint) {
	t.Helper()
	req := &fwksched.InferenceRequest{RequestID: "bind-" + sessionID, Headers: map[string]string{"x-session-id": sessionID}}
	require.NoError(t, sa.PreRequest(context.Background(), req, &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults:     map[string]*fwksched.ProfileRunResult{"default": {TargetEndpoints: []fwksched.Endpoint{ep}}},
	}))
}

func sessionReq(sessionID string) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{RequestID: "req-" + sessionID, Headers: map[string]string{"x-session-id": sessionID}}
}

func counter(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(labels...))
}

func gauge(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(labels...))
}

// fakeLookup is a BindingLookup with fixed counts that records how often the
// full-scan BindingCounts is called.
type fakeLookup struct {
	mu          sync.Mutex
	counts      map[string]sessionutil.BindingCounts
	countsCalls int
	panicCounts bool
}

func (f *fakeLookup) ResolveSessionID(context.Context, *fwksched.InferenceRequest) string { return "" }
func (f *fakeLookup) BoundEndpoint(string, time.Time) (string, bool)                      { return "", false }
func (f *fakeLookup) BindingCounts(time.Duration, time.Time) map[string]sessionutil.BindingCounts {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.countsCalls++
	if f.panicCounts {
		panic("boom")
	}
	return f.counts
}

func (f *fakeLookup) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.countsCalls
}

// ------------------------------------------------------------------ config

func TestFactory_DefaultsAndValidation(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	assert.Equal(t, Parameters{BurstMinutes: 5, RecentBindingWindowSeconds: 600}, p.params)
	assert.Equal(t, fwkplugin.TypedName{Type: PluginType, Name: "adm"}, p.TypedName())

	handle := testHandle(t)
	for _, bad := range []string{
		`{"burstMinutes":0}`, `{"burstMinutes":-1}`, `{"minAdvantageTokens":-1}`,
		`{"recentBindingWindowSeconds":0}`, `{"recentBindingWindowSeconds":-5}`, `{"unknownField":1}`,
	} {
		_, err := Factory("adm", fwkplugin.StrictDecoder(json.RawMessage(bad)), handle)
		assert.Error(t, err, bad)
	}
	_, err := Factory("adm", nil, nil)
	assert.ErrorContains(t, err, "handle")
}

func TestFactory_SessionAffinityReference(t *testing.T) { // review focus 2
	handle := testHandle(t)
	_, err := Factory("adm", fwkplugin.StrictDecoder(json.RawMessage(`{"sessionAffinityPluginName":"missing"}`)), handle)
	require.ErrorContains(t, err, "missing")

	enc, err := sessionaffinityfilter.Factory("enc", nil, handle) // strategy encoded_endpoint_header: no store
	require.NoError(t, err)
	handle.AddPlugin("enc", enc)
	_, err = Factory("adm", fwkplugin.StrictDecoder(json.RawMessage(`{"sessionAffinityPluginName":"enc"}`)), handle)
	require.ErrorContains(t, err, "session_id")

	other, err := Factory("other", nil, handle) // a plugin that is not a session-affinity-filter
	require.NoError(t, err)
	handle.AddPlugin("other", other)
	_, err = Factory("adm", fwkplugin.StrictDecoder(json.RawMessage(`{"sessionAffinityPluginName":"other"}`)), handle)
	require.ErrorContains(t, err, "is not an instance of")
}

func TestConfigParser_ExposesThePluginRefForTheLoaderDAG(t *testing.T) {
	cfg, err := ConfigParser(fwkplugin.StrictDecoder(json.RawMessage(`{"sessionAffinityPluginName":"sa"}`)), nil)
	require.NoError(t, err)
	assert.Equal(t, "sa", cfg.(*Parameters).SessionAffinityPluginName)
	_, err = ConfigParser(fwkplugin.StrictDecoder(json.RawMessage(`{"nope":1}`)), nil)
	assert.Error(t, err)
	_, err = ConfigParser(fwkplugin.StrictDecoder(json.RawMessage(`{"burstMinutes":0}`)), nil)
	assert.Error(t, err)
}

func TestConsumes_DeclaresPrefixAlwaysAndInFlightOnlyOnPrefixPlanes(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	req := p.Consumes().Required
	assert.Contains(t, req, attrprefix.PrefixCacheMatchInfoDataKey)
	assert.Contains(t, req, attrconcurrency.InFlightLoadDataKey)
	p, _ = newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	req = p.Consumes().Required
	assert.Contains(t, req, attrprefix.PrefixCacheMatchInfoDataKey)
	assert.NotContains(t, req, attrconcurrency.InFlightLoadDataKey)
}

// ------------------------------------------------------------------ Filter

func TestFilter_NoLabelsIsIdentity(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	in := []fwksched.Endpoint{testEndpoint("a", nil, 0), testEndpoint("b", nil, 4)}
	p.random = func() float64 { t.Fatal("rng must not be drawn"); return 0 }
	out := p.Filter(context.Background(), &fwksched.InferenceRequest{}, in)
	assert.Equal(t, in, out)
	assert.Equal(t, 0, testutil.CollectAndCount(endpointLoad), "no lifecycle label: no per-decision series")
}

func TestFilter_EmptyInputIsReturnedAsIs(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	assert.Empty(t, p.Filter(context.Background(), &fwksched.InferenceRequest{}, nil))
}

func TestFilter_RetainWithSessionBindings(t *testing.T) {
	p, sa := newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	p.now = time.Now // bindings are stamped with the wall clock
	d := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 0)
	a := testEndpoint("a", nil, 0)
	bind(t, sa, "s1", d)

	assert.Equal(t, []string{"d", "a"}, names(p.Filter(context.Background(), sessionReq("s1"), []fwksched.Endpoint{d, a})))
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), sessionReq("s2"), []fwksched.Endpoint{d, a})))
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "d", "draining", "retain", "retained"))
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "d", "draining", "retain", "steered"))
}

func TestFilter_BoundToAbsentEndpointIsUnbound(t *testing.T) { // review focus 5
	p, sa := newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	p.now = time.Now
	gone := testEndpoint("gone", nil, 0)
	d := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 0)
	a := testEndpoint("a", nil, 0)
	bind(t, sa, "s1", gone) // the health gate removed "gone" from the candidates
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), sessionReq("s1"), []fwksched.Endpoint{d, a})))
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "d", "draining", "retain", "steered"))

	// Unbound means placeable: a warming endpoint may take the request. Passing the
	// stale binding through to decide would block the draw (a bound request never draws).
	w := testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 0)
	assert.Equal(t, []string{"w"}, names(p.Filter(context.Background(), sessionReq("s1"), []fwksched.Endpoint{w, a})))
}

func TestFilter_MinAdvantageDefaultsToOneBlock(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	d := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 5)
	a := testEndpoint("a", nil, 4) // advantage = 1 block = 256 tokens >= minAdvantage(256): retained
	assert.Equal(t, []string{"d", "a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{d, a})))
	// equal matches: advantage 0 < one block, steered (an unresolved 0 would retain it)
	even := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 4)
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{even, a})))
	p, _ = newTestPlugin(t, `{"minAdvantageTokens":300}`, false)
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{d, a})))
}

func TestFilter_PrefixMatchInfoMissing(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	d := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "d"}, Name: "d",
		Labels: map[string]string{LabelState: "draining", LabelPhase: "retain"}}, &fwkdl.Metrics{}, nil)
	a := testEndpoint("a", nil, 0)
	// d has no match info -> match 0, advantage 0 < one block: steered
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{d, a})))
	// nobody has match info -> minAdvantage falls back to 1 token; still steered, still never empty
	b := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "b"}, Name: "b"}, &fwkdl.Metrics{}, nil)
	assert.Equal(t, []string{"b"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{d, b})))
}

func TestFilter_MalformedLabelCountsAndIsActive(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	before := testutil.ToFloat64(labelErrorsTotal)
	x := testEndpoint("x", map[string]string{LabelState: "warming", LabelShare: "0.25 "}, 0)
	a := testEndpoint("a", nil, 0)
	assert.Equal(t, []string{"x", "a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{x, a})))
	assert.Equal(t, before+1, testutil.ToFloat64(labelErrorsTotal))
}

func TestFilter_MetricsUseTheEndpointIDName(t *testing.T) { // ruling P3
	p, _ := newTestPlugin(t, "", false)
	d := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "pod-0-rank-0"}, Name: "pod-0",
		Labels: map[string]string{LabelState: "draining", LabelPhase: "retain"}}, &fwkdl.Metrics{}, nil)
	a := testEndpoint("a", nil, 0)
	p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{d, a})
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "pod-0-rank-0", "draining", "retain", "steered"))
	assert.Equal(t, 0.0, counter(t, decisionsTotal, "pod-0", "draining", "retain", "steered"))
}

func TestFilter_WarmingUsesInFlightOnPrefixPlanes(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	w := withInFlight(testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 0), 5)
	a := withInFlight(testEndpoint("a", nil, 0), 4)
	b := withInFlight(testEndpoint("b", nil, 0), 4)
	assert.Equal(t, []string{"w", "a", "b"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a, b})))
	assert.Equal(t, 5.0, gauge(t, endpointLoad, "w"))
	assert.Equal(t, 4.0, gauge(t, endpointLoad, "a"))
	w = withInFlight(w, 4)
	assert.Equal(t, []string{"w"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a, b})))
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "w", "warming", "", "forced"))
}

func TestFilter_WarmingFallsBackToScrapedRunningRequests(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	mk := func(name string, labels map[string]string, running int) fwksched.Endpoint {
		ep := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: name}, Name: name, Labels: labels},
			&fwkdl.Metrics{RunningRequestsSize: running}, nil)
		ep.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(0, 20, testBlock))
		return ep
	}
	w := mk("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 9)
	a := mk("a", nil, 2)
	assert.Equal(t, []string{"w", "a"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a})))
	assert.Equal(t, 9.0, gauge(t, endpointLoad, "w"))
}

func TestFilter_WarmingUsesRecentBindingsOnSessionPlanes(t *testing.T) {
	p, sa := newTestPlugin(t, `{"sessionAffinityPluginName":"sa","recentBindingWindowSeconds":600}`, true)
	p.now = time.Now // bindings were stamped with the wall clock
	w := withInFlight(testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 0), 0)
	a := withInFlight(testEndpoint("a", nil, 0), 9) // in-flight is ignored on session planes
	for _, s := range []string{"s1", "s2", "s3"} {
		bind(t, sa, s, w)
	}
	bind(t, sa, "s4", a)
	p.refresh(ids("w", "a")) // the 15 s loop caches the binding counts Filter reads
	// w has 3 recent bindings, a has 1: 3 < 1+1 is false -> overload stop
	assert.Equal(t, []string{"w", "a"}, names(p.Filter(context.Background(), sessionReq("new"), []fwksched.Endpoint{w, a})))
	assert.Equal(t, 3.0, gauge(t, endpointLoad, "w"))
	assert.Equal(t, 1.0, gauge(t, endpointLoad, "a"))
}

func TestFilter_SessionPlaneNeverScansBindingsPerRequest(t *testing.T) { // carry: BindingCounts only in refresh
	p, _ := newTestPlugin(t, "", false)
	lookup := &fakeLookup{counts: map[string]sessionutil.BindingCounts{"default/w": {Total: 2, Recent: 1}, "default/a": {Total: 7, Recent: 5}}}
	p.bindings = lookup // a session plane, with a lookup that counts full scans
	w := testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 0)
	a := testEndpoint("a", nil, 0)

	// before the first refresh the cache is empty: every load reads 0 and w is forced
	assert.Equal(t, []string{"w"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a})))
	p.refresh(ids("w", "a"))
	require.Equal(t, 1, lookup.calls())
	for range 5 {
		// cached: w 1 < 5 + 1 -> forced
		assert.Equal(t, []string{"w"}, names(p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a})))
	}
	assert.Equal(t, 1, lookup.calls(), "Filter reads the cached counts, never BindingCounts")
	assert.Equal(t, 1.0, gauge(t, endpointLoad, "w"))
	assert.Equal(t, 5.0, gauge(t, endpointLoad, "a"))
}

func TestFilter_SessionPlaneLeavesTheLoadGaugeToRefreshWhenNothingIsWarming(t *testing.T) { // ruling P6
	p, sa := newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	p.now = time.Now
	d := withInFlight(testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 0), 9)
	a := withInFlight(testEndpoint("a", nil, 0), 9)
	bind(t, sa, "s1", d)
	bind(t, sa, "s2", d)
	p.Filter(context.Background(), sessionReq("s3"), []fwksched.Endpoint{d, a})
	assert.Equal(t, 0, testutil.CollectAndCount(endpointLoad), "Filter publishes no load series when nothing is warming")
	p.refresh(ids("d", "a"))
	p.Filter(context.Background(), sessionReq("s3"), []fwksched.Endpoint{d, a})
	assert.Equal(t, 2.0, gauge(t, endpointLoad, "d"), "bindings, not the in-flight count")
	assert.Equal(t, 0.0, gauge(t, endpointLoad, "a"))
}

func TestFilter_MigrateMovesWithinBudgetAndRecordsTokens(t *testing.T) {
	p, _ := newTestPlugin(t, `{"burstMinutes":1}`, false)
	d := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "60000"}, 14)
	a := testEndpoint("a", nil, 4) // advantage 10 blocks = 2560 tokens
	req := &fwksched.InferenceRequest{}
	t0 := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return t0 }
	assert.Equal(t, []string{"d", "a"}, names(p.Filter(context.Background(), req, []fwksched.Endpoint{d, a})), "bucket starts empty: deferred")
	assert.Equal(t, 1.0, counter(t, decisionsTotal, "d", "draining", "migrate", "deferred"))
	p.now = func() time.Time { return t0.Add(3 * time.Second) } // 1000 tok/s * 3 s = 3000 >= 2560
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), req, []fwksched.Endpoint{d, a})))
	assert.Equal(t, 2560.0, counter(t, movedTokensTotal, "d"))
	assert.InDelta(t, 440.0, gauge(t, bucketTokens, "d"), 1e-6)
}

func TestFilter_BucketFollowsLabels(t *testing.T) { // review focus 1
	p, _ := newTestPlugin(t, `{"burstMinutes":1}`, false)
	t0 := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return t0 }
	migrate := func(budget string) fwksched.Endpoint {
		return testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: budget}, 14)
	}
	a := testEndpoint("a", nil, 4)
	req := &fwksched.InferenceRequest{}

	// create the bucket, then put it in debt with an oversized move once it has filled
	p.Filter(context.Background(), req, []fwksched.Endpoint{migrate("60000"), a})
	p.now = func() time.Time { return t0.Add(time.Hour) }
	big := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "60000"}, 400) // 396 blocks ~ 101k > cap 60k
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), req, []fwksched.Endpoint{big, a})))
	assert.Less(t, gauge(t, bucketTokens, "d"), 0.0, "in debt")

	// (a) budget label edited -> bucket recreated EMPTY under the new budget
	assert.Equal(t, []string{"d", "a"}, names(p.Filter(context.Background(), req, []fwksched.Endpoint{migrate("120000"), a})))
	assert.Equal(t, 0.0, gauge(t, bucketTokens, "d"))
	assert.Equal(t, int64(120000), p.buckets["default/d"].budgetTPM)

	// (b) phase flipped back to retain -> bucket dropped, series gone
	retain := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "retain"}, 14)
	p.Filter(context.Background(), req, []fwksched.Endpoint{retain, a})
	_, has := p.buckets["default/d"]
	assert.False(t, has)
	assert.Equal(t, 0, testutil.CollectAndCount(bucketTokens))

	// (c) back to migrate -> starts empty again (not the old debt)
	assert.Equal(t, []string{"d", "a"}, names(p.Filter(context.Background(), req, []fwksched.Endpoint{migrate("60000"), a})))
	assert.Equal(t, 0.0, gauge(t, bucketTokens, "d"))

	// (d) state removed entirely while in debt -> bucket dropped
	p.now = func() time.Time { return t0.Add(2 * time.Hour) }
	p.Filter(context.Background(), req, []fwksched.Endpoint{big, a})
	assert.Less(t, gauge(t, bucketTokens, "d"), 0.0)
	p.Filter(context.Background(), req, []fwksched.Endpoint{testEndpoint("d", nil, 14), a})
	_, has = p.buckets["default/d"]
	assert.False(t, has)
}

func TestFilter_ConcurrentMigrateIsRaceFreeAndBounded(t *testing.T) { // review focus 3; run with -race
	p, _ := newTestPlugin(t, `{"burstMinutes":1}`, false)
	t0 := time.Unix(1_700_000_000, 0)
	migrating := func() []fwksched.Endpoint {
		d := testEndpoint("d", map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "60000"}, 14)
		a := testEndpoint("a", nil, 4) // 2560 tokens per move
		return []fwksched.Endpoint{d, a}
	}
	// Ruling P4: the bucket is created EMPTY by a first decision at t0 (deferred); an
	// hour later it refills to capacity (60k) on the next take, and the clock then
	// stands still, so the workers share exactly one full bucket.
	p.now = func() time.Time { return t0 }
	p.Filter(context.Background(), &fwksched.InferenceRequest{}, migrating())
	p.now = func() time.Time { return t0.Add(time.Hour) }

	const workers, perWorker = 16, 50
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				p.Filter(context.Background(), &fwksched.InferenceRequest{}, migrating())
			}
		}()
	}
	wg.Wait()
	moved := counter(t, decisionsTotal, "d", "draining", "migrate", "moved")
	deferred := counter(t, decisionsTotal, "d", "draining", "migrate", "deferred")
	assert.Equal(t, 23.0, moved, "floor(60000 / 2560) moves fit one full bucket")
	assert.Equal(t, 23*2560.0, counter(t, movedTokensTotal, "d"))
	assert.LessOrEqual(t, counter(t, movedTokensTotal, "d"), 60000.0, "a full bucket admits at most its capacity of fitting moves")
	assert.Equal(t, float64(workers*perWorker+1), moved+deferred, "one decision per call, plus the priming one")
}

func TestFilter_RecoversPanicsAndPassesThrough(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	p.random = func() float64 { panic("boom") }
	before := testutil.ToFloat64(panicsTotal)
	w := testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "1"}, 0)
	a := testEndpoint("a", nil, 0)
	in := []fwksched.Endpoint{w, a}
	assert.Equal(t, in, p.Filter(context.Background(), &fwksched.InferenceRequest{}, in))
	assert.Equal(t, before+1, testutil.ToFloat64(panicsTotal))
}

func TestFilter_EndpointLoadGaugeIsSetAtEveryDecision(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	w := withInFlight(testEndpoint("w", map[string]string{LabelState: "warming", LabelShare: "0"}, 0), 3)
	a := withInFlight(testEndpoint("a", nil, 0), 1)
	p.Filter(context.Background(), &fwksched.InferenceRequest{}, []fwksched.Endpoint{w, a})
	assert.Equal(t, 3.0, gauge(t, endpointLoad, "w"))
	assert.Equal(t, 1.0, gauge(t, endpointLoad, "a"))
}

// ------------------------------------------------------- hooks and refresh

func TestResponseHeader_CountsErrorsPerEndpoint(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	target := &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "d"}, Name: "d"}
	for _, status := range []string{"200", "502", "502", "429", "abc", "999"} {
		p.ResponseHeader(context.Background(), &fwksched.InferenceRequest{}, &fwkrc.Response{Headers: map[string]string{":status": status}}, target)
	}
	p.ResponseHeader(context.Background(), &fwksched.InferenceRequest{}, &fwkrc.Response{Headers: map[string]string{"status": "503"}}, target)
	p.ResponseHeader(context.Background(), &fwksched.InferenceRequest{}, &fwkrc.Response{}, target) // no headers
	p.ResponseHeader(context.Background(), nil, nil, nil)                                           // tolerated
	assert.Equal(t, 3, testutil.CollectAndCount(endpointErrorsTotal), "series for 502, 429 and 503 only (no 200, abc or 999)")
	assert.Equal(t, 2.0, counter(t, endpointErrorsTotal, "d", "502"))
	assert.Equal(t, 1.0, counter(t, endpointErrorsTotal, "d", "429"), "4xx is counted with its own code; the manager filters 5xx")
	assert.Equal(t, 1.0, counter(t, endpointErrorsTotal, "d", "503"))
}

func TestResponseHeader_UsesTheEndpointIDName(t *testing.T) { // ruling P3
	p, _ := newTestPlugin(t, "", false)
	target := &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "pod-0-rank-0"}, Name: "pod-0"}
	p.ResponseHeader(context.Background(), &fwksched.InferenceRequest{}, &fwkrc.Response{Headers: map[string]string{":status": "500"}}, target)
	assert.Equal(t, 1.0, counter(t, endpointErrorsTotal, "pod-0-rank-0", "500"))
}

func TestRefresh_SessionGaugesAndEndpointLoad(t *testing.T) {
	p, sa := newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	w := testEndpoint("w", nil, 0)
	bind(t, sa, "s1", w)
	bind(t, sa, "s2", w)
	p.now = time.Now
	p.refresh(ids("w", "idle"))
	assert.Equal(t, 2.0, gauge(t, sessionBindings, "w"))
	assert.Equal(t, 2.0, gauge(t, sessionBindingsRecent, "w"))
	assert.Equal(t, 2.0, gauge(t, endpointLoad, "w"))
	assert.Equal(t, 0.0, gauge(t, sessionBindings, "idle"), "every listed endpoint has a series")
	assert.Equal(t, 0.0, gauge(t, endpointLoad, "idle"))
}

func TestRefresh_PrefixPlaneReadsTheInFlightProducerAndPrunesBuckets(t *testing.T) {
	p, _ := newTestPlugin(t, `{"burstMinutes":1}`, false)
	p.inFlight = func(id string) int64 { return map[string]int64{"default/a": 7}[id] }
	p.buckets["default/gone"] = newTokenBucket(1, 1, time.Unix(0, 0))
	bucketTokens.WithLabelValues("gone").Set(1)
	p.refresh(ids("a", "b"))
	assert.Equal(t, 7.0, gauge(t, endpointLoad, "a"))
	assert.Equal(t, 0.0, gauge(t, endpointLoad, "b"))
	_, has := p.buckets["default/gone"]
	assert.False(t, has, "buckets of endpoints that left the pool are pruned")
	assert.Equal(t, 0, testutil.CollectAndCount(bucketTokens))
}

func TestRefresh_PrefixPlaneLoadIsTheInFlightProducersCount(t *testing.T) { // ruling P5
	p, _ := newTestPlugin(t, "", false)
	handle := testHandle(t)
	plg, err := inflightload.InFlightLoadProducerFactory(inflightload.InFlightLoadProducerType, nil, handle)
	require.NoError(t, err)
	producer := plg.(*inflightload.InFlightLoadProducer)
	handle.AddPlugin(inflightload.InFlightLoadProducerType, producer) // what CreateMissingDataProducers does

	meta := &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "a"}, Name: "a"}
	server := fwkdl.NewEndpoint(meta, nil)
	require.NoError(t, producer.Extract(handle.Context(), fwkdl.EndpointEvent{Type: fwkdl.EventAddOrUpdate, Endpoint: server}))
	ep := fwksched.NewEndpoint(server.GetMetadata(), server.GetMetrics(), server.GetAttributes())
	for _, id := range []string{"r1", "r2"} {
		require.NoError(t, producer.PreRequest(handle.Context(), &fwksched.InferenceRequest{RequestID: id}, &fwksched.SchedulingResult{
			PrimaryProfileName: "default",
			ProfileResults:     map[string]*fwksched.ProfileRunResult{"default": {TargetEndpoints: []fwksched.Endpoint{ep}}},
		}))
	}

	assert.Equal(t, 2.0, p.loadOf(ep), "per decision: the producer's InFlightLoad attribute")
	p.resolveInFlight(handle)
	require.NotNil(t, p.inFlight)
	p.refresh([]k8stypes.NamespacedName{meta.ID})
	assert.Equal(t, 2.0, gauge(t, endpointLoad, "a"), "the refresh reads the same count through GetRequests")
}

func TestRefresh_DropsSeriesOfEndpointsThatLeftThePool(t *testing.T) { // ruling P7
	p, _ := newTestPlugin(t, `{"sessionAffinityPluginName":"sa"}`, true)
	p.bindings = &fakeLookup{counts: map[string]sessionutil.BindingCounts{"default/a": {Total: 2, Recent: 1}, "default/b": {Total: 3, Recent: 3}}}
	p.buckets["default/a"] = newTokenBucket(60, 1, time.Unix(0, 0))
	p.buckets["default/b"] = newTokenBucket(60, 1, time.Unix(0, 0))
	p.refresh(ids("a", "b"))
	for _, vec := range []*prometheus.GaugeVec{endpointLoad, sessionBindings, sessionBindingsRecent, bucketTokens} {
		assert.Equal(t, 2, testutil.CollectAndCount(vec))
	}
	p.refresh(ids("a")) // b left the pool
	for _, vec := range []*prometheus.GaugeVec{endpointLoad, sessionBindings, sessionBindingsRecent, bucketTokens} {
		assert.Equal(t, 1, testutil.CollectAndCount(vec))
	}
	assert.Equal(t, 1.0, gauge(t, endpointLoad, "a"))
	assert.Equal(t, 2.0, gauge(t, sessionBindings, "a"))
}

func TestRefresh_ARefreshPanicIsRecovered(t *testing.T) {
	p, _ := newTestPlugin(t, "", false)
	p.bindings = &fakeLookup{panicCounts: true}
	before := testutil.ToFloat64(panicsTotal)
	handle := testHandle(t)
	assert.NotPanics(t, func() { p.refreshOnce(handle.Context(), handle) })
	assert.Equal(t, before+1, testutil.ToFloat64(panicsTotal))
}
