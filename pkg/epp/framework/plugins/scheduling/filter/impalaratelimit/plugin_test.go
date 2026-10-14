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

package impalaratelimit

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	"github.com/llm-d/llm-d-router/test/utils"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func resetMetrics() {
	engineRate.Reset()
	limitGauge.Reset()
	unmeasuredGauge.Reset()
	overGauge.Reset()
	droppedTotal.Reset()
	labelErrorsTotal.Reset()
}

func testHandle(t *testing.T) fwkplugin.Handle {
	t.Helper()
	ctx, cancel := context.WithCancel(utils.NewTestContext(t))
	t.Cleanup(cancel)
	return utils.NewTestHandle(ctx)
}

// newTestPlugin builds a 60 s-window plugin directly (no publish loop) with a fake clock.
func newTestPlugin(t *testing.T) (*Plugin, *fakeClock) {
	t.Helper()
	resetMetrics()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	return newPlugin("rl", Parameters{WindowSeconds: 60}, clock.Now), clock
}

func meta(name string, labels map[string]string) *fwkdl.EndpointMetadata {
	return &fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: name}, Name: name, Labels: labels}
}

func candidate(name string, labels map[string]string) fwksched.Endpoint {
	return fwksched.NewEndpoint(meta(name, labels), &fwkdl.Metrics{}, nil)
}

func names(eps []fwksched.Endpoint) []string {
	out := make([]string, len(eps))
	for i, e := range eps {
		out[i] = e.GetMetadata().ID.Name
	}
	return out
}

func ids(ns ...string) []k8stypes.NamespacedName {
	out := make([]k8stypes.NamespacedName, len(ns))
	for i, n := range ns {
		out[i] = k8stypes.NamespacedName{Namespace: "default", Name: n}
	}
	return out
}

// poll runs one Extract for endpoint `name` with the given exposition text.
func poll(t *testing.T, p *Plugin, name string, labels map[string]string, text string) {
	t.Helper()
	in := fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{
		Payload:  parseText(t, text),
		Endpoint: fwkdl.NewEndpoint(meta(name, labels), nil),
	}
	require.NoError(t, p.Extract(context.Background(), in))
}

// steady polls once per second for `seconds` seconds with counters growing at the given
// per-minute rates from zero, then leaves the clock one second after the last poll.
func steady(t *testing.T, p *Plugin, clock *fakeClock, name string, labels map[string]string,
	rpm, promptPM, genPM, hitsPM float64, seconds int) {
	t.Helper()
	for s := 0; s <= seconds; s++ {
		m := float64(s) / 60
		// A histogram sample count is an integer: the text parser reads a fractional one as 0.
		poll(t, p, name, labels, vllmText(math.Round(rpm*m), promptPM*m, genPM*m, hitsPM*m))
		clock.Advance(time.Second)
	}
}

// series returns the value of each series of collector c for endpoint "a", keyed by its
// `dimension` label ("" for collectors without one). Absent series are absent keys.
func series(t *testing.T, c prometheus.Collector) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			lbl := map[string]string{}
			for _, l := range m.GetLabel() {
				lbl[l.GetName()] = l.GetValue()
			}
			if lbl[labelEndpoint] != "a" {
				continue
			}
			v := m.GetGauge().GetValue()
			if m.GetCounter() != nil {
				v = m.GetCounter().GetValue()
			}
			out[lbl[labelDimension]] = v
		}
	}
	return out
}

func TestFactory_DefaultsAndBounds(t *testing.T) {
	h := testHandle(t)
	p, err := Factory("rl", nil, h)
	require.NoError(t, err)
	assert.Equal(t, fwkplugin.TypedName{Type: PluginType, Name: "rl"}, p.TypedName())
	assert.Equal(t, 60*time.Second, p.(*Plugin).window)

	for _, raw := range []string{`{"windowSeconds": 9}`, `{"windowSeconds": 601}`, `{"windowSeconds": 0}`, `{"windowSeconds": "60"}`} {
		_, err := Factory("rl", json.NewDecoder(strings.NewReader(raw)), h)
		assert.Error(t, err, raw)
	}
	for _, raw := range []string{`{"windowSeconds": 10}`, `{"windowSeconds": 600}`} {
		_, err := Factory("rl", json.NewDecoder(strings.NewReader(raw)), h)
		assert.NoError(t, err, raw)
	}
	_, err = Factory("rl", nil, nil)
	assert.Error(t, err, "a handle is required")
}

type captureRegistrar struct{ regs []fwkdl.PendingRegistration }

func (c *captureRegistrar) Register(r fwkdl.PendingRegistration) error {
	c.regs = append(c.regs, r)
	return nil
}

func TestRegisterDependencies_AttachesToTheMetricsSourceAndWarnsIfMissing(t *testing.T) {
	p, _ := newTestPlugin(t)
	reg := &captureRegistrar{}
	require.NoError(t, p.RegisterDependencies(reg))
	require.Len(t, reg.regs, 1)
	r := reg.regs[0]
	assert.Equal(t, sourcemetrics.MetricsDataSourceType, r.SourceType)
	assert.Equal(t, fwkdl.Warn, r.IfMissing)
	assert.Nil(t, r.DefaultSource, "never auto-create a second metrics source")
	assert.Same(t, p, r.Extractor)
	assert.Equal(t, p.TypedName(), r.Owner)
}

func TestFilter_DropsAnEngineAtItsRPMLimit(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "60"}
	steady(t, p, clock, "a", lim, 60, 0, 0, 0, 60)
	steady(t, p, clock, "b", nil, 600, 0, 0, 0, 0) // b carries no limit
	got := p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)})
	assert.Equal(t, []string{"b"}, names(got), "a measured 60 rpm >= its limit 60")
	assert.Equal(t, 1.0, testutil.ToFloat64(droppedTotal.WithLabelValues("a")))
}

func TestFilter_KeepsAnEngineBelowItsRPMLimit(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "61"}
	steady(t, p, clock, "a", lim, 60, 0, 0, 0, 60)
	got := p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)})
	assert.Equal(t, []string{"a", "b"}, names(got))
}

func TestFilter_TPMCountsUncachedPlusGeneratedNotCachedTokens(t *testing.T) {
	// prompt 10000/min of which 8000 cached, 1000 generated: TPM = 2000 + 1000 = 3000
	// (total tokens would be 11000).
	for _, tc := range []struct {
		limit string
		keep  bool
	}{{"3000", false}, {"3001", true}, {"5000", true}} {
		p, clock := newTestPlugin(t)
		lim := map[string]string{LabelLimitTPM: tc.limit}
		steady(t, p, clock, "a", lim, 0, 10_000, 1_000, 8_000, 60)
		got := p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)})
		assert.Equal(t, tc.keep, len(got) == 2, "limit %s", tc.limit)
	}
}

func TestFilter_UnmeasuredNeverDrops(t *testing.T) {
	lim := map[string]string{LabelLimitRPM: "1", LabelLimitTPM: "1"}
	others := func() []fwksched.Endpoint { return []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)} }

	p, _ := newTestPlugin(t) // no polls at all
	assert.Len(t, p.Filter(context.Background(), nil, others()), 2, "never polled")

	p, clock := newTestPlugin(t)
	steady(t, p, clock, "a", lim, 600, 600, 600, 0, 20) // 20 s < window/2
	assert.Len(t, p.Filter(context.Background(), nil, others()), 2, "warming up")

	p, clock = newTestPlugin(t)
	steady(t, p, clock, "a", lim, 600, 600, 600, 0, 60)
	clock.Advance(16 * time.Second) // newest sample now 17 s old > window/4
	assert.Len(t, p.Filter(context.Background(), nil, others()), 2, "stale")
}

func TestFilter_MissingLocalHitsLeavesOnlyTPMUnmeasured(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitTPM: "1"}
	for s := 0; s <= 60; s++ {
		m := float64(s) / 60
		poll(t, p, "a", lim, strings.Split(vllmText(600*m, 600*m, 600*m, 0), "# TYPE vllm:prefix_cache_hits_total")[0])
		clock.Advance(time.Second)
	}
	assert.Len(t, p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)}), 2)
	p.publishOnce(ids("a", "b"))
	assert.Equal(t, map[string]float64{dimTPM: 1}, series(t, unmeasuredGauge))
	assert.Equal(t, 600.0, series(t, engineRate)[dimRPM], "RPM is still measured")
}

func TestFilter_MalformedLabelDisablesOnlyThatDimension(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "lots", LabelLimitTPM: "100"}
	steady(t, p, clock, "a", lim, 600, 0, 600, 0, 60) // tpm 600 >= 100
	got := p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)})
	assert.Equal(t, []string{"b"}, names(got), "the valid TPM limit still applies")
	assert.Equal(t, 1.0, testutil.ToFloat64(labelErrorsTotal.WithLabelValues("a")))
}

func TestFilter_FailsOpenWhenEveryCandidateIsOver(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "10"}
	steady(t, p, clock, "a", lim, 60, 0, 0, 0, 60)
	clock.Advance(-61 * time.Second) // replay the same minute for b
	steady(t, p, clock, "b", lim, 60, 0, 0, 0, 60)
	before := testutil.ToFloat64(failOpenTotal)
	in := []fwksched.Endpoint{candidate("a", lim), candidate("b", lim)}
	got := p.Filter(context.Background(), nil, in)
	assert.Equal(t, []string{"a", "b"}, names(got))
	assert.Equal(t, 1.0, testutil.ToFloat64(failOpenTotal)-before)
	assert.Empty(t, series(t, droppedTotal), "nothing was dropped")
}

func TestFilter_EmptyInput(t *testing.T) {
	p, _ := newTestPlugin(t)
	assert.Empty(t, p.Filter(context.Background(), nil, nil))
}

func TestFilter_RecoversPanics(t *testing.T) {
	p, _ := newTestPlugin(t)
	p.now = func() time.Time { panic("boom") }
	before := testutil.ToFloat64(panicsTotal)
	in := []fwksched.Endpoint{candidate("a", map[string]string{LabelLimitRPM: "1"})}
	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), nil, in)))
	assert.Equal(t, 1.0, testutil.ToFloat64(panicsTotal)-before)
}

func TestExtract_DownsamplesToOneSamplePerSecond(t *testing.T) {
	p, clock := newTestPlugin(t)
	for range 20 {
		poll(t, p, "a", nil, vllmText(1, 1, 1, 0))
		clock.Advance(50 * time.Millisecond)
	}
	assert.Len(t, p.endpoints[meta("a", nil).ID.String()].win.samples, 1)
}

func TestExtract_CounterResetMakesTheEngineUnmeasuredUntilWarm(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "10"}
	steady(t, p, clock, "a", lim, 60, 0, 0, 0, 60)
	in := func() []fwksched.Endpoint { return []fwksched.Endpoint{candidate("a", lim), candidate("b", nil)} }
	require.Len(t, p.Filter(context.Background(), nil, in()), 1, "over before the restart")
	poll(t, p, "a", lim, vllmText(0, 0, 0, 0)) // engine restarted
	clock.Advance(time.Second)
	assert.Len(t, p.Filter(context.Background(), nil, in()), 2, "unmeasured right after the restart")
}

func TestPublish_GaugesForALimitedEngine(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "60", LabelLimitTPM: "100000"}
	steady(t, p, clock, "a", lim, 60, 10_000, 1_000, 8_000, 60)
	p.publishOnce(ids("a"))
	assert.Equal(t, map[string]float64{dimRPM: 60, dimTPM: 3_000}, series(t, engineRate))
	assert.Equal(t, map[string]float64{dimRPM: 60, dimTPM: 100_000}, series(t, limitGauge))
	assert.Equal(t, map[string]float64{dimRPM: 0, dimTPM: 0}, series(t, unmeasuredGauge))
	assert.Equal(t, map[string]float64{dimRPM: 1, dimTPM: 0}, series(t, overGauge))
}

func TestPublish_EngineRateWithoutLimitsIsStillPublished(t *testing.T) {
	p, clock := newTestPlugin(t)
	steady(t, p, clock, "a", nil, 30, 0, 0, 0, 60)
	p.publishOnce(ids("a"))
	assert.Equal(t, 30.0, series(t, engineRate)[dimRPM], "calibration data with no limit set")
	assert.Empty(t, series(t, limitGauge))
	assert.Empty(t, series(t, unmeasuredGauge))
	assert.Empty(t, series(t, overGauge))
}

func TestPublish_LimitedEndpointWithNoPollsIsReportedUnmeasured(t *testing.T) {
	p, _ := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "5"}
	p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", lim)}) // seen by traffic only
	p.publishOnce(ids("a"))
	assert.Equal(t, map[string]float64{dimRPM: 1}, series(t, unmeasuredGauge))
	assert.Equal(t, map[string]float64{dimRPM: 5}, series(t, limitGauge))
	assert.Empty(t, series(t, engineRate))
}

func TestPublish_RemovingALimitDropsItsSeries(t *testing.T) {
	p, clock := newTestPlugin(t)
	steady(t, p, clock, "a", map[string]string{LabelLimitRPM: "5"}, 30, 0, 0, 0, 60)
	p.publishOnce(ids("a"))
	require.NotEmpty(t, series(t, limitGauge))
	p.Filter(context.Background(), nil, []fwksched.Endpoint{candidate("a", nil)}) // label removed
	p.publishOnce(ids("a"))
	assert.Empty(t, series(t, limitGauge))
	assert.Empty(t, series(t, overGauge))
}

func TestPublish_PrunesEndpointsThatLeftThePool(t *testing.T) {
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "5"}
	steady(t, p, clock, "a", lim, 30, 0, 0, 0, 60)
	p.publishOnce(ids("a", "b"))
	require.NotEmpty(t, series(t, engineRate))
	p.publishOnce(ids("b"))
	assert.Empty(t, series(t, engineRate))
	assert.Empty(t, series(t, limitGauge))
	assert.NotContains(t, p.endpoints, meta("a", nil).ID.String())
}

func TestPublish_AnEmptyPodListPrunesNothing(t *testing.T) {
	// At startup the datastore is empty until the config has loaded: that must not wipe state.
	p, clock := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "5"}
	steady(t, p, clock, "a", lim, 30, 0, 0, 0, 60)
	p.publishOnce(ids("a"))
	require.Equal(t, 30.0, series(t, engineRate)[dimRPM])

	p.publishOnce(nil)
	assert.Contains(t, p.endpoints, meta("a", nil).ID.String(), "the endpoint state survives")
	assert.Equal(t, 30.0, series(t, engineRate)[dimRPM], "its series survive and are republished")
	assert.Equal(t, 5.0, series(t, limitGauge)[dimRPM])
}

func TestPublishLoop_PublishesPeriodicallyAndStopsWithTheContext(t *testing.T) {
	p, _ := newTestPlugin(t)
	var calls atomic.Int64
	podList := func() []k8stypes.NamespacedName { calls.Add(1); return ids("a") }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { p.publishLoop(ctx, podList, 5*time.Millisecond); close(done) }()

	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"the loop publishes on every tick")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishLoop did not return after its context was cancelled")
	}
	settled := calls.Load()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, settled, calls.Load(), "no publish after the loop returned")
}

func TestFactory_StartsThePublishLoopOnTheHandleContext(t *testing.T) {
	ctx, cancel := context.WithCancel(utils.NewTestContext(t))
	defer cancel()
	_, err := Factory("rl", nil, utils.NewTestHandle(ctx))
	require.NoError(t, err)
}

func TestFilter_APanicInsideTheLockedSectionDoesNotWedgeThePlugin(t *testing.T) {
	p, _ := newTestPlugin(t)
	lim := map[string]string{LabelLimitRPM: "1"}
	// A state with no window makes st.win.rates panic while p.mu is held.
	p.endpoints[meta("a", nil).ID.String()] = &endpointState{name: "a"}
	in := []fwksched.Endpoint{candidate("a", lim)}
	before := testutil.ToFloat64(panicsTotal)

	assert.Equal(t, []string{"a"}, names(p.Filter(context.Background(), nil, in)))
	assert.Equal(t, 1.0, testutil.ToFloat64(panicsTotal)-before)

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Filter(context.Background(), nil, in)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Filter blocked: the mutex was left locked by the panic")
	}
	assert.Equal(t, 2.0, testutil.ToFloat64(panicsTotal)-before)
	// Extract takes the same mutex; it must not block either.
	extracted := make(chan struct{})
	go func() {
		defer close(extracted)
		_ = p.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{
			Payload:  parseText(t, vllmText(1, 1, 1, 0)),
			Endpoint: fwkdl.NewEndpoint(meta("b", nil), nil),
		})
	}()
	select {
	case <-extracted:
	case <-time.After(2 * time.Second):
		t.Fatal("Extract blocked: the mutex was left locked by the panic")
	}
}

func TestTransitionLog_OneLinePerThirtySecondsAndTheFinalStateIsNeverLost(t *testing.T) {
	p, clock := newTestPlugin(t)
	var mu sync.Mutex
	var lines []string
	p.logger = funcr.New(func(_, args string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, args)
	}, funcr.Options{Verbosity: 0})
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(lines) }
	last := func() string { mu.Lock(); defer mu.Unlock(); return lines[len(lines)-1] }

	over := map[string]string{LabelLimitRPM: "60"}    // measured 60 rpm: at the limit
	roomy := map[string]string{LabelLimitRPM: "1000"} // the same traffic is far below it
	steady(t, p, clock, "a", over, 60, 0, 0, 0, 60)   // leaves the counter at 60
	next := 61.0
	step := func(lbl map[string]string) {
		poll(t, p, "a", lbl, vllmText(next, 0, 0, 0))
		next++
		clock.Advance(time.Second)
		p.publishOnce(ids("a"))
	}

	// (a) the first transition is logged at once.
	step(over)
	require.Equal(t, 1, count())
	assert.Contains(t, last(), "at its limit")
	assert.Contains(t, last(), `"suppressedTransitions"=0`)
	loggedAt := clock.Now()

	// (b) flipping back within 30 s logs nothing.
	step(roomy)
	assert.Equal(t, 1, count())

	// (d, part 1) and nothing while the state holds and 30 s have not passed.
	for clock.Now().Sub(loggedAt) < 29*time.Second {
		step(roomy)
		require.Equal(t, 1, count(), "at %s after the first line", clock.Now().Sub(loggedAt))
	}

	// (c) once 30 s have passed the CURRENT state is logged, with the suppressed count.
	for i := 0; i < 5 && count() == 1; i++ {
		step(roomy)
	}
	require.Equal(t, 2, count())
	assert.Contains(t, last(), "stopped being limited")
	assert.NotContains(t, last(), `"suppressedTransitions"=0`)
	assert.GreaterOrEqual(t, clock.Now().Sub(loggedAt), 30*time.Second)

	// (d) no further lines while the state is unchanged.
	for range 40 {
		step(roomy)
	}
	assert.Equal(t, 2, count())
}
