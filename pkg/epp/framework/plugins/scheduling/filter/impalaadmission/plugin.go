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
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload"
	sessionaffinityfilter "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/sessionaffinity"
	sessionutil "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/util/sessionaffinity"
)

// PluginType is the type string a scheduling profile references.
const PluginType = "impala-admission-filter"

const (
	// refreshInterval is the steady-state cadence of the gauge refresh loop.
	refreshInterval = 15 * time.Second
	// startupRefreshInterval is how often the loop checks for the first listed
	// endpoint, so the contract series appear about one second after the pool
	// is known rather than one refreshInterval later.
	startupRefreshInterval = time.Second
	statusPseudoHeader     = ":status"
	statusHeader           = "status"
)

var (
	_ fwksched.Filter               = &Plugin{}
	_ fwkplugin.ConsumerPlugin      = &Plugin{}
	_ fwkrc.ResponseHeaderProcessor = &Plugin{}
)

// Parameters is the plugin's JSON configuration (contract names).
type Parameters struct {
	// MinAdvantageTokens: a draining endpoint keeps an unbound request only
	// when its prefix advantage is at least this many tokens. 0 = one prefix
	// block (PrefixCacheMatchInfo.BlockSizeTokens()).
	MinAdvantageTokens int `json:"minAdvantageTokens,omitempty"`
	// BurstMinutes: migrate token-bucket capacity = budget-tpm x BurstMinutes.
	BurstMinutes float64 `json:"burstMinutes,omitempty"`
	// SessionAffinityPluginName: declared name of the session-affinity-filter
	// (strategy session_id) whose bindings this plugin reads. Empty = prefix-only
	// plane. The pluginRef tag makes the config loader construct it first and
	// reject a dangling name at load time (configloader.go buildPluginDAG
	// @v0.10.0).
	SessionAffinityPluginName string `json:"sessionAffinityPluginName,omitempty" pluginRef:""`
	// RecentBindingWindowSeconds: a binding counts as warming load (and as
	// "recent" for impala_session_bindings_recent) when refreshed within it.
	RecentBindingWindowSeconds int `json:"recentBindingWindowSeconds,omitempty"`
}

func defaultParameters() Parameters {
	return Parameters{BurstMinutes: 5, RecentBindingWindowSeconds: 600}
}

func (p *Parameters) validate() error {
	if p.MinAdvantageTokens < 0 {
		return fmt.Errorf("minAdvantageTokens must be >= 0, got %d", p.MinAdvantageTokens)
	}
	if p.BurstMinutes <= 0 {
		return fmt.Errorf("burstMinutes must be > 0 (it sizes the migrate bucket), got %v", p.BurstMinutes)
	}
	if p.RecentBindingWindowSeconds <= 0 {
		return fmt.Errorf("recentBindingWindowSeconds must be > 0, got %d", p.RecentBindingWindowSeconds)
	}
	return nil
}

func decodeParameters(raw *json.Decoder) (Parameters, error) {
	params := defaultParameters()
	if raw != nil {
		if err := raw.Decode(&params); err != nil {
			return params, fmt.Errorf("failed to parse the parameters of the '%s' filter - %w", PluginType, err)
		}
	}
	if err := params.validate(); err != nil {
		return params, fmt.Errorf("invalid parameters of the '%s' filter - %w", PluginType, err)
	}
	return params, nil
}

// ConfigParser is the plugin.ConfigParserFunc registered with
// RegisterWithPluginDependencies: the loader reads the pluginRef-tagged
// SessionAffinityPluginName from the returned struct to order construction.
func ConfigParser(raw *json.Decoder, _ fwkplugin.Handle) (any, error) {
	params, err := decodeParameters(raw)
	if err != nil {
		return nil, err
	}
	return &params, nil
}

// Plugin is the impala-admission-filter.
type Plugin struct {
	typedName      fwkplugin.TypedName
	params         Parameters
	prefixMatchKey fwkplugin.DataKey
	inFlightKey    fwkplugin.DataKey
	bindings       sessionutil.BindingLookup // nil on prefix-only planes
	// recentBindings is BindingCounts(...).Recent per endpoint ID, cached by
	// the refresh loop for Filter: BindingCounts scans every binding, which is
	// too costly per request.
	recentBindings atomic.Pointer[map[string]int]
	// inFlight is the in-flight-load producer's GetRequests, read by the
	// refresh loop on prefix planes. Resolved lazily by (and confined to) the
	// refresh goroutine; nil until then.
	inFlight func(endpointID string) int64
	now      func() time.Time
	random   func() float64

	mu      sync.Mutex
	buckets map[string]*tokenBucket // endpoint ID string -> migrate bucket

	// labelErrLogged maps an endpoint ID string to the label error last logged
	// for it, so a persistent bad label is logged at Info once, not per
	// request. Entries are cleared when the endpoint parses cleanly or leaves
	// the pool, so it holds at most one entry per currently malformed endpoint.
	labelErrLogged sync.Map
}

// Factory builds the plugin, resolves the session-affinity instance (fails
// at config load when it is missing, of another type, or not strategy
// session_id), registers the metrics and starts the refresh loop for the
// handle context's lifetime.
func Factory(name string, raw *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, fmt.Errorf("'%s' filter: a plugin handle is required", PluginType)
	}
	params, err := decodeParameters(raw)
	if err != nil {
		return nil, err
	}
	p := &Plugin{
		typedName:      fwkplugin.TypedName{Type: PluginType, Name: name},
		params:         params,
		prefixMatchKey: attrprefix.PrefixCacheMatchInfoDataKey,
		inFlightKey:    attrconcurrency.InFlightLoadDataKey,
		now:            time.Now,
		random:         rand.Float64,
		buckets:        map[string]*tokenBucket{},
	}
	if params.SessionAffinityPluginName != "" {
		sa, err := fwkplugin.PluginByType[*sessionaffinityfilter.SessionAffinity](handle, params.SessionAffinityPluginName)
		if err != nil {
			return nil, fmt.Errorf("'%s' filter: sessionAffinityPluginName: %w", PluginType, err)
		}
		lookup, ok := sa.BindingLookup()
		if !ok {
			return nil, fmt.Errorf("'%s' filter: sessionAffinityPluginName %q is not a session-affinity-filter with strategy session_id",
				PluginType, params.SessionAffinityPluginName)
		}
		p.bindings = lookup
	}
	if err := registerMetrics(handle.Metrics()); err != nil {
		return nil, err
	}
	ctx := handle.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	go p.refreshLoop(ctx, handle, startupRefreshInterval, refreshInterval)
	return p, nil
}

func (p *Plugin) TypedName() fwkplugin.TypedName { return p.typedName }

// Consumes declares PrefixCacheMatchInfo always, and InFlightLoad only on
// prefix-only planes; CreateMissingDataProducers instantiates the default
// producers for whatever the profile does not declare.
func (p *Plugin) Consumes() fwkplugin.DataDependencies {
	required := map[fwkplugin.DataKey]any{p.prefixMatchKey: attrprefix.PrefixCacheMatchInfo{}}
	if p.bindings == nil {
		required[p.inFlightKey] = attrconcurrency.InFlightLoad{}
	}
	return fwkplugin.DataDependencies{Required: required}
}

// Filter applies the §5.3 rules. It never returns an empty list and recovers
// its own panics, returning the input unchanged.
func (p *Plugin) Filter(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) (result []fwksched.Endpoint) {
	defer func() {
		if r := recover(); r != nil {
			panicsTotal.Inc()
			log.FromContext(ctx).Error(fmt.Errorf("%v", r), "impala-admission-filter panicked; passing candidates through", "plugin", p.typedName.String())
			result = endpoints
		}
	}()
	if len(endpoints) == 0 {
		return endpoints
	}

	cands, minAdvantage, lifecycleSeen := p.collect(ctx, endpoints)
	p.reconcileBuckets(cands)
	if !lifecycleSeen {
		return endpoints
	}
	p.observeLoads(cands)

	kept, decisions := decide(decideInput{
		cands:        cands,
		boundID:      p.boundCandidate(ctx, request, cands),
		minAdvantage: minAdvantage,
		random:       p.random,
		take:         p.take,
		refund:       p.refund,
	})
	logger := log.FromContext(ctx).V(logutil.DEBUG)
	for _, d := range decisions {
		decisionsTotal.WithLabelValues(d.endpoint, d.state, d.phase, d.kind).Inc()
		if d.kind == decisionMoved && d.movedTokens > 0 {
			movedTokensTotal.WithLabelValues(d.endpoint).Add(d.movedTokens)
		}
		logger.Info("impala-admission-filter decision", "endpoint", d.endpoint,
			"state", d.state, "phase", d.phase, "decision", d.kind, "movedTokens", d.movedTokens)
	}
	if len(kept) == 0 { // decide guarantees non-empty; belt and braces for the contract
		return endpoints
	}
	out := make([]fwksched.Endpoint, 0, len(kept))
	for _, i := range kept {
		out = append(out, endpoints[i])
	}
	return out
}

// collect parses labels and reads match info for every candidate. It returns
// the effective minimum advantage, always > 0 (configured, else one block from
// the first candidate carrying match info, else 1 token so that "no
// information" never counts as "belongs"), and whether any candidate carries a
// lifecycle state. On prefix planes it also reads each candidate's load; on
// session planes observeLoads fills it from the cached bindings.
func (p *Plugin) collect(ctx context.Context, endpoints []fwksched.Endpoint) ([]candidate, float64, bool) {
	cands := make([]candidate, len(endpoints))
	blockSize, lifecycleSeen := 0, false
	for i, ep := range endpoints {
		md := ep.GetMetadata()
		lc, err := parseLifecycle(md.Labels)
		p.noteLabelError(ctx, md, err)
		if lc.State != StateActive {
			lifecycleSeen = true
		}
		var match float64
		if raw, ok := ep.Get(p.prefixMatchKey.String()); ok {
			if info, ok := raw.(*attrprefix.PrefixCacheMatchInfo); ok && info != nil {
				// CachedBlockCount is the unweighted block count (data_types.go @v0.10.0).
				match = float64(info.CachedBlockCount() * info.BlockSizeTokens())
				if blockSize == 0 {
					blockSize = info.BlockSizeTokens()
				}
			}
		}
		// Ruling P3: the metrics `endpoint` label is ID.Name everywhere.
		cands[i] = candidate{id: md.ID.String(), name: md.ID.Name, lc: lc, match: match}
		if p.bindings == nil {
			cands[i].load = p.loadOf(ep)
		}
	}
	minAdvantage := float64(p.params.MinAdvantageTokens)
	if minAdvantage == 0 {
		minAdvantage = float64(blockSize)
	}
	if minAdvantage <= 0 {
		minAdvantage = 1
	}
	return cands, minAdvantage, lifecycleSeen
}

// noteLabelError counts a malformed-label candidate evaluation and names the
// endpoint at Info the first time it (or a different error on it) is seen. The
// counter has no endpoint label (the gateway's CATALOG expects it bare), so the
// log line is the only way to find the offender; it is deduplicated because
// this runs per candidate per request while a bad label persists. A clean parse
// forgets the endpoint, so a relapse is reported again.
func (p *Plugin) noteLabelError(ctx context.Context, md *fwkdl.EndpointMetadata, err error) {
	key := md.ID.String()
	if err == nil {
		if _, seen := p.labelErrLogged.Load(key); seen {
			p.labelErrLogged.Delete(key)
		}
		return
	}
	labelErrorsTotal.Inc()
	msg := err.Error()
	if prev, seen := p.labelErrLogged.Load(key); seen && prev == msg {
		return
	}
	p.labelErrLogged.Store(key, msg)
	log.FromContext(ctx).Info("impala-admission-filter: malformed lifecycle label, treating the endpoint as active",
		"endpoint", md.ID.Name, "error", msg)
}

// loadOf is the prefix-plane load: the in-flight-load producer's request
// count (a DynamicAttribute installed by its Extract, producer.go @v0.10.0)
// when present, else the scraped running count.
func (p *Plugin) loadOf(ep fwksched.Endpoint) float64 {
	if raw, ok := ep.Get(p.inFlightKey.String()); ok {
		if load, ok := raw.(*attrconcurrency.InFlightLoad); ok && load != nil {
			return float64(load.Requests)
		}
	}
	if m := ep.GetMetrics(); m != nil {
		return float64(m.RunningRequestsSize)
	}
	return 0
}

func anyWarming(cands []candidate) bool {
	for i := range cands {
		if cands[i].lc.State == StateWarming {
			return true
		}
	}
	return false
}

// observeLoads publishes impala_admission_endpoint_load per decision. On
// session planes the load is the refresh loop's cached recent-binding count
// and is set only while a candidate is warming (the only time decide reads
// it); otherwise the refresh loop alone owns the gauge, so the series never
// flips between two measures (ruling P6).
func (p *Plugin) observeLoads(cands []candidate) {
	if p.bindings != nil {
		if !anyWarming(cands) {
			return
		}
		var recent map[string]int
		if cached := p.recentBindings.Load(); cached != nil {
			recent = *cached
		}
		for i := range cands {
			cands[i].load = float64(recent[cands[i].id])
		}
	}
	for i := range cands {
		endpointLoad.WithLabelValues(cands[i].name).Set(cands[i].load)
	}
}

func (p *Plugin) recentWindow() time.Duration {
	return time.Duration(p.params.RecentBindingWindowSeconds) * time.Second
}

// boundCandidate returns the ID of the candidate the request's session is
// bound to, or "" (no session plane, no session id, unbound, expired, or
// bound to an endpoint that is not a candidate — e.g. removed by the health
// gate — which the rules treat as unbound).
func (p *Plugin) boundCandidate(ctx context.Context, request *fwksched.InferenceRequest, cands []candidate) string {
	if p.bindings == nil || request == nil {
		return ""
	}
	sessionID := p.bindings.ResolveSessionID(ctx, request)
	if sessionID == "" {
		return ""
	}
	id, ok := p.bindings.BoundEndpoint(sessionID, p.now())
	if !ok {
		return ""
	}
	for i := range cands {
		if cands[i].id == id {
			return id
		}
	}
	return ""
}

// take is decide's bucket callback: one bucket per migrating endpoint,
// created EMPTY on first sight and recreated empty when the budget label
// changes. Goroutine-safe.
func (p *Plugin) take(c candidate, cost float64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	b := p.buckets[c.id]
	if b == nil || b.budgetTPM != c.lc.BudgetTPM {
		b = newTokenBucket(c.lc.BudgetTPM, p.params.BurstMinutes, now)
		p.buckets[c.id] = b
	}
	ok := b.take(cost, now)
	bucketTokens.WithLabelValues(c.name).Set(b.level())
	return ok
}

// refund undoes a take that decide's never-empty restore made moot: the request
// stays on the draining endpoint, so it must not spend the migrate budget. The
// level is capped at capacity (a full bucket may have gone negative on an
// oversized move, and that debt is returned too). A bucket pruned in between
// (the endpoint stopped migrating) has nothing to refund.
func (p *Plugin) refund(c candidate, cost float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.buckets[c.id]
	if b == nil || b.budgetTPM != c.lc.BudgetTPM {
		return
	}
	b.refund(cost)
	bucketTokens.WithLabelValues(c.name).Set(b.level())
}

// reconcileBuckets drops the bucket (and its gauge series) of every candidate
// that is no longer in draining/migrate, so a later migrate starts empty and
// a retracted migrate forgives any debt. Buckets of endpoints absent from the
// candidates are left alone and pruned by the refresh loop once the endpoint
// leaves the pool. Note that pruning on the refresh loop's view means one
// inventory tick without the endpoint (a transient file-discovery gap) drops
// its bucket: the debt is forgiven and, when the endpoint returns, the bucket
// restarts EMPTY. That is the conservative direction, because a fresh bucket
// can only move less than a surviving one.
func (p *Plugin) reconcileBuckets(cands []candidate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range cands {
		c := cands[i]
		if c.lc.State == StateDraining && c.lc.Phase == PhaseMigrate {
			continue
		}
		if _, has := p.buckets[c.id]; has {
			delete(p.buckets, c.id)
			bucketTokens.DeleteLabelValues(c.name)
		}
	}
}

// ResponseHeader counts model-server responses with an HTTP status in 400-599
// per serving endpoint (impala_endpoint_errors_total{endpoint,code}); `code`
// is the numeric status, so consumers filter 5xx themselves. Envoy delivers
// the status as the ":status" pseudo-header (handlers/response.go @v0.10.0);
// a plain "status" key is accepted too.
func (p *Plugin) ResponseHeader(_ context.Context, _ *fwksched.InferenceRequest, response *fwkrc.Response, target *fwkdl.EndpointMetadata) {
	if response == nil || target == nil {
		return
	}
	raw, ok := response.Headers[statusPseudoHeader]
	if !ok {
		raw, ok = response.Headers[statusHeader]
	}
	if !ok {
		return
	}
	code, err := strconv.Atoi(raw)
	if err != nil || code < 400 || code > 599 {
		return
	}
	endpointErrorsTotal.WithLabelValues(target.ID.Name, strconv.Itoa(code)).Inc()
}

// refreshLoop republishes the per-endpoint gauges until ctx ends. It refreshes
// as soon as the datastore lists an endpoint, then every interval. The pool is
// checked at once and then every startupInterval, because the factory runs
// during config load and file discovery or the pod reconciler fill the
// datastore only afterwards (runner.go @v0.10.0), so the list is normally
// still empty at start. Without this, every EPP restart would leave the
// contract series absent for one interval, which llmd-manager reads as "plugin
// not running". Until that first refresh there is nothing to publish, so the
// startup checks touch no gauge.
func (p *Plugin) refreshLoop(ctx context.Context, handle fwkplugin.Handle, startupInterval, interval time.Duration) {
	pods, ok := awaitPods(ctx, handle, startupInterval)
	if !ok {
		return
	}
	p.refreshOnce(ctx, handle, pods)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.refreshOnce(ctx, handle, handle.PodList())
		}
	}
}

// awaitPods returns the first non-empty pod list, checking immediately and
// then every interval; ok is false when ctx ends first.
func awaitPods(ctx context.Context, handle fwkplugin.Handle, interval time.Duration) ([]k8stypes.NamespacedName, bool) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-timer.C:
			if pods := handle.PodList(); len(pods) > 0 {
				return pods, true
			}
			timer.Reset(interval)
		}
	}
}

// refreshOnce runs one refresh over pods; a panic is counted and the refresh
// skipped, because a panic on this goroutine would otherwise end the EPP
// process. Prefix planes retry the producer lookup on every refresh until it
// resolves.
func (p *Plugin) refreshOnce(ctx context.Context, handle fwkplugin.Handle, pods []k8stypes.NamespacedName) {
	defer func() {
		if r := recover(); r != nil {
			panicsTotal.Inc()
			log.FromContext(ctx).Error(fmt.Errorf("%v", r), "impala-admission-filter refresh panicked; skipped", "plugin", p.typedName.String())
		}
	}()
	if p.bindings == nil && p.inFlight == nil {
		p.resolveInFlight(handle)
	}
	p.refresh(pods)
}

// resolveInFlight finds the default in-flight-load producer, which
// CreateMissingDataProducers creates AFTER this plugin's factory ran
// (runner.go @v0.10.0), named by its type. It reads the handle's plugin map,
// which has no lock and is written only during config load. The refresh loop
// calls it only after the datastore has listed an endpoint. Discovery starts
// only after config load, so the datastore's synchronisation orders this read
// after every write to the map.
func (p *Plugin) resolveInFlight(handle fwkplugin.Handle) {
	if producer, ok := handle.Plugin(inflightload.InFlightLoadProducerType).(*inflightload.InFlightLoadProducer); ok && producer != nil {
		p.inFlight = producer.GetRequests
	}
}

// refresh republishes the per-endpoint gauges for exactly the endpoints in
// pods (0 when idle), dropping the series of endpoints that left the pool
// (ruling P7), caches the recent-binding counts for Filter on session planes,
// and prunes the buckets of endpoints that left the pool.
func (p *Plugin) refresh(pods []k8stypes.NamespacedName) {
	present := make(map[string]string, len(pods)) // ID string -> ID.Name
	for _, id := range pods {
		present[id.String()] = id.Name
	}

	// Read everything before resetting, so a failed read leaves the old series.
	var counts map[string]sessionutil.BindingCounts
	if p.bindings != nil {
		counts = p.bindings.BindingCounts(p.recentWindow(), p.now())
		recent := make(map[string]int, len(counts))
		for id, c := range counts {
			recent[id] = c.Recent
		}
		p.recentBindings.Store(&recent)
	}

	endpointLoad.Reset()
	sessionBindings.Reset()
	sessionBindingsRecent.Reset()
	for id, name := range present {
		if p.bindings != nil {
			c := counts[id]
			sessionBindings.WithLabelValues(name).Set(float64(c.Total))
			sessionBindingsRecent.WithLabelValues(name).Set(float64(c.Recent))
			endpointLoad.WithLabelValues(name).Set(float64(c.Recent))
			continue
		}
		var load int64
		if p.inFlight != nil {
			load = p.inFlight(id)
		}
		endpointLoad.WithLabelValues(name).Set(float64(load))
	}

	p.labelErrLogged.Range(func(key, _ any) bool {
		if _, ok := present[key.(string)]; !ok {
			p.labelErrLogged.Delete(key)
		}
		return true
	})

	p.mu.Lock()
	defer p.mu.Unlock()
	bucketTokens.Reset()
	for id, b := range p.buckets {
		name, ok := present[id]
		if !ok {
			// A one-tick inventory drop lands here too: the bucket (and any debt)
			// is forgotten and restarts empty if the endpoint comes back, which
			// can only move less than the surviving bucket would (conservative).
			delete(p.buckets, id)
			continue
		}
		bucketTokens.WithLabelValues(name).Set(b.level())
	}
}
