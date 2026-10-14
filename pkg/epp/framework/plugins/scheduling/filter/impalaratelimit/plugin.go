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
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

// PluginType is the type string an EppConfig declares and a scheduling profile references.
const PluginType = "impala-rate-limit-filter"

const (
	defaultWindowSeconds = 60
	minWindowSeconds     = 10
	maxWindowSeconds     = 600
	// publishInterval is how often the gauges are republished from the plugin's state.
	publishInterval = 5 * time.Second
	// transitionLogEvery caps the limited/unlimited INFO lines per endpoint.
	transitionLogEvery = 30 * time.Second
)

var (
	_ fwksched.Filter                                           = &Plugin{}
	_ fwkdl.PollingExtractor[sourcemetrics.PrometheusMetricMap] = &Plugin{}
	_ fwkdl.Registrant                                          = &Plugin{}
)

// Parameters is the plugin's JSON configuration.
type Parameters struct {
	// WindowSeconds is the rolling window the rates are measured over.
	WindowSeconds int `json:"windowSeconds,omitempty"`
}

func decodeParameters(raw *json.Decoder) (Parameters, error) {
	params := Parameters{WindowSeconds: defaultWindowSeconds}
	if raw != nil {
		if err := raw.Decode(&params); err != nil {
			return params, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", PluginType, err)
		}
	}
	if params.WindowSeconds < minWindowSeconds || params.WindowSeconds > maxWindowSeconds {
		return params, fmt.Errorf("invalid parameters of the '%s' filter - windowSeconds must be in [%d, %d], got %d",
			PluginType, minWindowSeconds, maxWindowSeconds, params.WindowSeconds)
	}
	return params, nil
}

// endpointState is everything the plugin knows about one endpoint.
type endpointState struct {
	name       string // EndpointMetadata.ID.Name: the metrics `endpoint` label
	win        *window
	limits     limits // last parsed, by Extract (every stored sample) or Filter (every request)
	limited    bool   // current "over on some dimension", as of the last publish
	logged     bool   // the "limited" state the last transition line reported
	lastLog    time.Time
	suppressed int // transitions not logged since lastLog
}

// Plugin is impala-rate-limit-filter: the extractor role fills each endpoint's window from the
// metrics polls, the filter role drops candidates at their limit.
type Plugin struct {
	typedName fwkplugin.TypedName
	window    time.Duration
	now       func() time.Time
	logger    logr.Logger

	mu        sync.Mutex
	endpoints map[string]*endpointState // EndpointMetadata.ID.String()

	// labelErrLogged maps an endpoint ID string to the label error last logged for it, so a
	// persistent bad label is logged once, not per request (impalaadmission's pattern).
	labelErrLogged sync.Map
}

func newPlugin(name string, params Parameters, now func() time.Time) *Plugin {
	return &Plugin{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		window:    time.Duration(params.WindowSeconds) * time.Second,
		now:       now,
		logger:    logr.Discard(),
		endpoints: map[string]*endpointState{},
	}
}

// Factory builds the plugin, registers its metrics and starts the publish loop for the
// handle context's lifetime.
func Factory(name string, raw *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, fmt.Errorf("'%s' filter: a plugin handle is required", PluginType)
	}
	params, err := decodeParameters(raw)
	if err != nil {
		return nil, err
	}
	if err := registerMetrics(handle.Metrics()); err != nil {
		return nil, err
	}
	p := newPlugin(name, params, time.Now)
	ctx := handle.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	p.logger = log.FromContext(ctx)
	go p.publishLoop(ctx, handle.PodList, publishInterval)
	return p, nil
}

func (p *Plugin) TypedName() fwkplugin.TypedName { return p.typedName }

// RegisterDependencies attaches the plugin as an extractor to the metrics-data-source the
// loader injects into every config without explicit sources (config/loader/defaults.go
// ensureDataLayer). Warn, not Fail: a config whose own sources lack it still loads, the plugin
// then measures nothing and so never drops (fail open), and impala_rate_limit_unmeasured says so.
func (p *Plugin) RegisterDependencies(r fwkdl.Registrar) error {
	return r.Register(fwkdl.PendingRegistration{
		Owner:      p.TypedName(),
		SourceType: sourcemetrics.MetricsDataSourceType,
		Extractor:  p,
		IfMissing:  fwkdl.Warn,
	})
}

// Extract stores at most one sample per endpoint per second from a metrics poll.
func (p *Plugin) Extract(_ context.Context, in fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]) error {
	if in.Endpoint == nil {
		return nil
	}
	md := in.Endpoint.GetMetadata()
	if md == nil {
		return nil
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.stateLocked(md)
	if !st.win.due(now) {
		return nil
	}
	st.win.add(now, readCounters(in.Payload))
	st.limits, _ = parseLimits(md.Labels) // label errors are counted per candidate in Filter
	return nil
}

func (p *Plugin) stateLocked(md *fwkdl.EndpointMetadata) *endpointState {
	id := md.ID.String()
	st := p.endpoints[id]
	if st == nil {
		st = &endpointState{name: md.ID.Name, win: newWindow(p.window)}
		p.endpoints[id] = st
	}
	return st
}

// Filter drops every candidate whose measured rate is at or above a limit, unless that would
// drop all of them (fail open). It never returns an empty list for a non-empty input and
// recovers its own panics, returning the input unchanged.
func (p *Plugin) Filter(ctx context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) (result []fwksched.Endpoint) {
	defer func() {
		if r := recover(); r != nil {
			panicsTotal.Inc()
			log.FromContext(ctx).Error(fmt.Errorf("%v", r), "impala-rate-limit-filter panicked; passing candidates through", "plugin", p.typedName.String())
			result = endpoints
		}
	}()
	if len(endpoints) == 0 {
		return endpoints
	}
	now := p.now()
	lims := make([]limits, len(endpoints))
	for i, ep := range endpoints {
		var err error
		lims[i], err = parseLimits(ep.GetMetadata().Labels)
		p.noteLabelError(ctx, ep.GetMetadata(), err)
	}
	kept := p.decide(endpoints, lims, now)
	if len(kept) == 0 {
		failOpenTotal.Inc()
		return endpoints
	}
	return kept
}

// decide returns the candidates that are not over a limit and counts the dropped ones, or nil
// when every candidate is over (the caller then fails open and nothing is counted as dropped).
// Everything runs under the mutex, released by defer so a panic cannot leave it held, and the
// drops are counted before it is released so a concurrent prune cannot be followed by a
// resurrected series.
func (p *Plugin) decide(endpoints []fwksched.Endpoint, lims []limits, now time.Time) []fwksched.Endpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	over := make([]bool, len(endpoints))
	kept := make([]fwksched.Endpoint, 0, len(endpoints))
	for i, ep := range endpoints {
		st := p.stateLocked(ep.GetMetadata())
		st.limits = lims[i] // so the publish loop reports a limited endpoint even if its polls never succeed
		over[i] = overLimit(lims[i], st.win.rates(now))
		if !over[i] {
			kept = append(kept, ep)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	for i, ep := range endpoints {
		if over[i] {
			droppedTotal.WithLabelValues(ep.GetMetadata().ID.Name).Inc()
		}
	}
	return kept
}

func overLimit(l limits, r rates) bool {
	return (l.RPM > 0 && r.RPMMeasured && r.RPM >= float64(l.RPM)) ||
		(l.TPM > 0 && r.TPMMeasured && r.TPM >= float64(l.TPM))
}

// noteLabelError counts a malformed limit label and names the endpoint at Info the first time
// it (or a different error on it) is seen; a clean parse forgets the endpoint.
func (p *Plugin) noteLabelError(ctx context.Context, md *fwkdl.EndpointMetadata, err error) {
	key := md.ID.String()
	if err == nil {
		if _, seen := p.labelErrLogged.Load(key); seen {
			p.labelErrLogged.Delete(key)
		}
		return
	}
	labelErrorsTotal.WithLabelValues(md.ID.Name).Inc()
	msg := err.Error()
	if prev, seen := p.labelErrLogged.Load(key); seen && prev == msg {
		return
	}
	p.labelErrLogged.Store(key, msg)
	log.FromContext(ctx).Info("impala-rate-limit-filter: malformed limit label, that dimension is not limited",
		"endpoint", md.ID.Name, "error", msg)
}

func (p *Plugin) publishLoop(ctx context.Context, podList func() []k8stypes.NamespacedName, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publishOnce(podList())
		}
	}
}

// publishOnce republishes every tracked endpoint's gauges and forgets endpoints that left the
// pool. An empty pod list prunes nothing: at startup the datastore fills only after config
// load, and pruning then would just drop state the next poll recreates. A panic is counted and
// the publish skipped, because a panic on this goroutine would end the EPP process.
func (p *Plugin) publishOnce(pods []k8stypes.NamespacedName) {
	defer func() {
		if r := recover(); r != nil {
			panicsTotal.Inc()
			p.logger.Error(fmt.Errorf("%v", r), "impala-rate-limit-filter publish panicked; skipped", "plugin", p.typedName.String())
		}
	}()
	present := make(map[string]struct{}, len(pods))
	for _, id := range pods {
		present[id.String()] = struct{}{}
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, st := range p.endpoints {
		if _, ok := present[id]; !ok && len(pods) > 0 {
			deleteSeries(st.name)
			delete(p.endpoints, id)
			p.labelErrLogged.Delete(id)
			continue
		}
		p.publishLocked(st, now)
	}
}

func (p *Plugin) publishLocked(st *endpointState, now time.Time) {
	r := st.win.rates(now)
	limited := false
	for _, d := range []struct {
		dim      string
		limit    int64
		rate     float64
		measured bool
	}{{dimRPM, st.limits.RPM, r.RPM, r.RPMMeasured}, {dimTPM, st.limits.TPM, r.TPM, r.TPMMeasured}} {
		if d.measured {
			engineRate.WithLabelValues(st.name, d.dim).Set(d.rate)
		} else {
			engineRate.DeleteLabelValues(st.name, d.dim)
		}
		if d.limit == 0 {
			limitGauge.DeleteLabelValues(st.name, d.dim)
			unmeasuredGauge.DeleteLabelValues(st.name, d.dim)
			overGauge.DeleteLabelValues(st.name, d.dim)
			continue
		}
		over := d.measured && d.rate >= float64(d.limit)
		limited = limited || over
		limitGauge.WithLabelValues(st.name, d.dim).Set(float64(d.limit))
		unmeasuredGauge.WithLabelValues(st.name, d.dim).Set(boolf(!d.measured))
		overGauge.WithLabelValues(st.name, d.dim).Set(boolf(over))
	}
	p.noteTransitionLocked(st, limited, r, now)
}

// noteTransitionLocked logs when an endpoint starts or stops being limited, at most once per
// transitionLogEvery per endpoint. It compares the CURRENT state with the state the last line
// reported, so a line suppressed by the cap is not lost: the next publish after the cap
// expires logs the then-current state, with the number of transitions it covers. A flip back to
// the already-logged state needs no line.
func (p *Plugin) noteTransitionLocked(st *endpointState, limited bool, r rates, now time.Time) {
	changed := limited != st.limited
	st.limited = limited
	if limited == st.logged || (!st.lastLog.IsZero() && now.Sub(st.lastLog) < transitionLogEvery) {
		if changed {
			st.suppressed++
		}
		return
	}
	msg := "impala-rate-limit-filter: endpoint stopped being limited"
	if limited {
		msg = "impala-rate-limit-filter: endpoint is at its limit, new work goes elsewhere"
	}
	p.logger.Info(msg, "endpoint", st.name, "rpm", r.RPM, "tpm", r.TPM,
		"limitRPM", st.limits.RPM, "limitTPM", st.limits.TPM, "suppressedTransitions", st.suppressed)
	st.logged, st.lastLog, st.suppressed = limited, now, 0
}

func boolf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
