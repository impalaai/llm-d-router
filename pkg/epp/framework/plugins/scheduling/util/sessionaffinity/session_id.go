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

package sessionaffinity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/common/duration"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// DefaultSessionIDHeader is the header a SessionIDConfig reads when no sources
// are configured.
const DefaultSessionIDHeader = "x-session-id"

// SessionIDConfig configures the session_id strategy.
type SessionIDConfig struct {
	// Sources resolve the session identifier in priority order; the first
	// non-empty match wins. When empty, a single header source
	// DefaultSessionIDHeader is used.
	Sources []SessionIDSource `json:"sources"`
	// EvictionTTLSeconds is how long a session binding survives unused.
	EvictionTTLSeconds float64 `json:"evictionTtlSeconds"`
	// EvictionSweepSeconds is how often expired bindings are swept.
	EvictionSweepSeconds float64 `json:"evictionSweepSeconds"`
	// SyncCrossReplicaState controls whether session bindings are shared
	// across EPP replicas when the configured cross-replica syncer stores
	// per-key values. Unset defaults to true; set false to keep bindings local
	// to this replica.
	SyncCrossReplicaState *bool `json:"syncCrossReplicaState,omitempty"`
	// StoreTimeoutSeconds bounds the shared-store calls of one request in each
	// of Choose and PreRequest. 0 defaults to defaultStoreTimeoutSeconds.
	StoreTimeoutSeconds float64 `json:"storeTimeoutSeconds,omitempty"`
}

// SessionIDSource names one place a session identifier can be read from.
// Exactly one of Header or Attribute must be set.
type SessionIDSource struct {
	// Header is a request header name.
	Header string `json:"header"`
	// Attribute is a request-attribute key published by an upstream plugin.
	Attribute string `json:"attribute"`
	// Producer names the plugin publishing the attribute. Empty addresses a
	// producer-agnostic key such as "agent-identity"; set it to read one
	// producer's attribute, e.g. "session-id-producer".
	Producer string `json:"producer"`
}

// Default eviction schedule applied to a SessionIDConfig with unset fields:
// bindings unused for 300s are swept every 10s.
const (
	defaultEvictionTTLSeconds   = 300
	defaultEvictionSweepSeconds = 10
)

// ApplyDefaults fills unset fields with their defaults: an empty Sources reads
// DefaultSessionIDHeader, and a zero eviction schedule uses the standard TTL
// and sweep. Run after decode and before Validate. Defaults live here rather
// than in a pre-seeded struct so a JSON overlay replaces Sources instead of
// merging element-wise into a seeded slice.
func (c *SessionIDConfig) ApplyDefaults() {
	if len(c.Sources) == 0 {
		c.Sources = []SessionIDSource{{Header: DefaultSessionIDHeader}}
	}
	if c.EvictionTTLSeconds == 0 {
		c.EvictionTTLSeconds = defaultEvictionTTLSeconds
	}
	if c.EvictionSweepSeconds == 0 {
		c.EvictionSweepSeconds = defaultEvictionSweepSeconds
	}
}

// Validate rejects configs that would not select a valid session identifier or
// a usable eviction schedule. It expects a config already passed through
// applyDefaults.
func (c *SessionIDConfig) Validate() error {
	if err := ValidateSources(c.Sources); err != nil {
		return err
	}
	if err := duration.ValidateSeconds("evictionTtlSeconds", c.EvictionTTLSeconds); err != nil {
		return err
	}
	if err := duration.ValidateSeconds("evictionSweepSeconds", c.EvictionSweepSeconds); err != nil {
		return err
	}
	if c.StoreTimeoutSeconds == 0 {
		return nil
	}
	return duration.ValidateSeconds("storeTimeoutSeconds", c.StoreTimeoutSeconds)
}

// ValidateSources rejects a source that does not set exactly one of Header or
// Attribute.
func ValidateSources(sources []SessionIDSource) error {
	for i, source := range sources {
		if err := source.validate(); err != nil {
			return fmt.Errorf("sources[%d]: %w", i, err)
		}
	}
	return nil
}

func (s SessionIDSource) validate() error {
	if (s.Header == "") == (s.Attribute == "") {
		return errors.New("exactly one of header or attribute must be set")
	}
	return nil
}

// SessionIDHeader maps an opaque client-supplied session identifier to a pod
// via a bounded, TTL-evicted binding store. It does not place sessions: an
// unbound session, or one whose bound pod is absent from the candidate set,
// yields no preference, and the consumer decides where the request goes. A
// session whose bound pod is absent rebinds to the pod the request was routed
// to.
//
// With a cross-replica syncer that stores per-key values, each binding is also
// one key in that store, read on every request. The stored binding overrides
// the local one, and the local one answers when the store does not.
//
// It is the shared implementation behind the session-affinity scorer and
// filter: Choose resolves the target endpoint, PreRequest commits the binding,
// ResponseHeader is a no-op. Each plugin embeds it and adds only its own output
// method.
type SessionIDHeader struct {
	sources     []SessionIDSource
	profileName string
	// typedName is the owning scorer/filter's identity. Its name namespaces
	// the shared bindings.
	typedName plugin.TypedName
	// pluginKey keys the Choose->PreRequest presence handoff and labels the
	// plugin in logs; it is the owning scorer/filter's plugin type.
	pluginKey     plugin.StateKey
	ttl           time.Duration
	sweepInterval time.Duration
	// mu serializes each bind/migrate/adopt/evict sequence so a first-bind
	// cannot race a concurrent sweep and leave the binding store inconsistent.
	mu          sync.Mutex
	bindings    sync.Map // session identifier -> binding
	pluginState *plugin.PluginState
	handle      plugin.Handle

	syncCrossReplicaState bool
	storeTimeout          time.Duration
	pinStateKey           datalayer.StateKey
	warnNoStore           sync.Once
}

// Consumes declares the request attributes the configured sources read. They
// are optional: a source whose producer is absent leaves the identifier
// unresolved and the next source, or the header, answers instead.
//
// The value witness is nil because attributeString accepts any value of string
// kind, so no single concrete type describes the key.
func (s *SessionIDHeader) Consumes() plugin.DataDependencies {
	return SourcesConsumes(s.sources)
}

// SourcesConsumes declares the request attributes named by sources as optional
// dependencies; see SessionIDHeader.Consumes.
func SourcesConsumes(sources []SessionIDSource) plugin.DataDependencies {
	optional := map[plugin.DataKey]any{}
	for _, source := range sources {
		if source.Attribute == "" {
			continue
		}
		optional[plugin.NewDataKey(source.Attribute, "").WithNonEmptyProducerName(source.Producer)] = nil
	}
	if len(optional) == 0 {
		return plugin.DataDependencies{}
	}
	return plugin.DataDependencies{Optional: optional}
}

// boundPodPresence records, for one request, which pod the session was bound
// to and whether it was present in the candidate set scored. PreRequest reads
// this so it can tell a genuine absence from the picker merely choosing a
// different pod, and ignore an absence of a pod the session has since left.
type boundPodPresence struct {
	podName string
	present bool
	// shared reports that Choose read the shared store, and sharedPod is the
	// pod it held for the session, or "" when it held none.
	shared    bool
	sharedPod string
}

func (b *boundPodPresence) Clone() plugin.StateData {
	clone := *b
	return &clone
}

// NewSessionIDHeader builds the shared session_id strategy implementation and
// starts its eviction sweep for the plugin's lifetime. typedName is the owning
// plugin's identity.
func NewSessionIDHeader(config SessionIDConfig, profileName string, typedName plugin.TypedName, handle plugin.Handle) *SessionIDHeader {
	syncCrossReplicaState := true
	if config.SyncCrossReplicaState != nil {
		syncCrossReplicaState = *config.SyncCrossReplicaState
	}
	storeTimeoutSeconds := config.StoreTimeoutSeconds
	if storeTimeoutSeconds == 0 {
		storeTimeoutSeconds = defaultStoreTimeoutSeconds
	}
	s := &SessionIDHeader{
		sources:               config.Sources,
		profileName:           profileName,
		typedName:             typedName,
		pluginKey:             plugin.StateKey(typedName.Type),
		ttl:                   time.Duration(config.EvictionTTLSeconds * float64(time.Second)),
		sweepInterval:         time.Duration(config.EvictionSweepSeconds * float64(time.Second)),
		pluginState:           plugin.NewPluginState(handle.Context()),
		handle:                handle,
		syncCrossReplicaState: syncCrossReplicaState,
		storeTimeout:          time.Duration(storeTimeoutSeconds * float64(time.Second)),
		pinStateKey:           datalayer.StateKey("session-affinity:" + typedName.Name),
	}
	if recorder := handle.Metrics(); recorder != nil {
		if err := registerMetrics(recorder); err != nil {
			log.FromContext(handle.Context()).Error(err, "Session affinity - shared pin metrics are not exported", "plugin", typedName.String())
		}
	}
	go s.runEviction(handle.Context(), s.sweepInterval)
	return s
}

// binding is the pod a session is pinned to, and when that pin was last used.
type binding struct {
	podName  string
	lastSeen time.Time
}

// resolveSessionID returns the session identifier from the first configured
// source that yields a non-empty value, or "" when none do.
//
// Empty means the request expresses no session preference. No identifier is
// generated: a router-minted value is unique per request, so it could never
// match a stored binding.
func (s *SessionIDHeader) resolveSessionID(ctx context.Context, request *scheduling.InferenceRequest) string {
	return ResolveSessionID(ctx, request, s.sources)
}

// ResolveSessionID returns the trimmed session identifier from the first of
// sources that yields a non-empty value, or "" when none do.
func ResolveSessionID(ctx context.Context, request *scheduling.InferenceRequest, sources []SessionIDSource) string {
	if request == nil {
		return ""
	}
	for _, source := range sources {
		if source.Header != "" {
			if id := strings.TrimSpace(request.Headers[NormalizeHeader(source.Header)]); id != "" {
				return id
			}
			continue
		}
		if id, ok := attributeString(ctx, request, source); ok {
			if id = strings.TrimSpace(id); id != "" {
				return id
			}
		}
	}
	return ""
}

// attributeString reads a request attribute as a string. The source names the
// attribute and its producer in separate fields, so the key is built rather
// than parsed out of one string. Producers store their identifier under a
// named string type (e.g. session.SessionID), so a plain string type assertion
// would miss them; any value of string kind is accepted.
func attributeString(ctx context.Context, request *scheduling.InferenceRequest, source SessionIDSource) (string, bool) {
	key := plugin.NewDataKey(source.Attribute, "").WithNonEmptyProducerName(source.Producer)
	v, ok := request.GetAttribute(key)
	if !ok {
		return "", false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.String {
		log.FromContext(ctx).V(logging.DEBUG).Info("Session affinity - attribute is not a string, skipping source",
			"attribute", key.String())
		return "", false
	}
	return rv.String(), true
}

// Choose resolves the endpoint a request should be pinned to and records, for
// PreRequest, whether the session's bound pod was present in the candidate set.
// It returns (nil, false) when there are no candidates, the request carries no
// session identifier, the session is unbound, or its bound pod is absent from
// the candidate set: in each case the session expresses no usable preference
// and the consumer decides. It returns (pod, true) only for a session bound to
// a present candidate. A binding held in the shared store replaces the local
// one first; the request's own binding is committed in PreRequest.
func (s *SessionIDHeader) Choose(ctx context.Context, request *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) (scheduling.Endpoint, bool) {
	if len(endpoints) == 0 {
		return nil, false
	}

	sessionID := s.resolveSessionID(ctx, request)
	if sessionID == "" {
		return nil, false
	}

	sharedPod, shared := s.loadSharedPin(ctx, sessionID)
	if sharedPod != "" {
		s.bindLocal(sessionID, sharedPod)
	}

	podName := s.boundPod(sessionID)
	var target scheduling.Endpoint
	for _, endpoint := range endpoints {
		if podName != "" && endpoint.GetMetadata().ID.String() == podName {
			target = endpoint
		}
	}

	if podName != "" || shared {
		s.pluginState.Write(request.RequestID, s.pluginKey, &boundPodPresence{
			podName: podName, present: target != nil, shared: shared, sharedPod: sharedPod,
		})
	}

	if target == nil {
		return nil, false
	}
	return target, true
}

// boundPod returns the pod bound to sessionID, or "" when unbound. Expiry is
// enforced by the sweeper, not on read.
func (s *SessionIDHeader) boundPod(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	value, ok := s.bindings.Load(sessionID)
	if !ok {
		return ""
	}
	b, ok := value.(binding)
	if !ok {
		return ""
	}
	return b.podName
}

// PreRequest commits the binding of a routed request. A session migrates only
// when its bound pod is absent from the candidate set.
func (s *SessionIDHeader) PreRequest(ctx context.Context, request *scheduling.InferenceRequest, schedulingResult *scheduling.SchedulingResult) {
	sessionID := s.resolveSessionID(ctx, request)
	if sessionID == "" {
		return
	}
	podName := s.pickedPodName(schedulingResult)
	if podName == "" {
		return
	}

	present, err := plugin.ReadPluginStateKey[*boundPodPresence](s.pluginState, request.RequestID, s.pluginKey)
	if err != nil {
		present = nil
	}
	if present != nil && present.shared {
		if pinned, ok := s.commitSharedPin(ctx, sessionID, podName, present); ok {
			s.bindLocal(sessionID, pinned)
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fresh := binding{podName: podName, lastSeen: time.Now()}
	existingValue, loaded := s.bindings.LoadOrStore(sessionID, fresh)
	if !loaded {
		log.FromContext(ctx).V(logging.DEBUG).Info("Session affinity - bound session to pod",
			"plugin", string(s.pluginKey), "pod", podName)
		return
	}

	existing, ok := existingValue.(binding)
	if !ok {
		return
	}

	if existing.podName == podName {
		s.bindings.CompareAndSwap(sessionID, existingValue, fresh)
		return
	}

	// Migrate only on a confirmed absence of the currently bound pod, recorded by
	// this request's own Choose call. A missing record (e.g. a concurrent
	// first-bind race) is not treated as absence, and neither is a record about
	// a pod a concurrent request has already migrated the session away from.
	if present == nil || present.present || present.podName != existing.podName {
		return
	}

	if !s.bindings.CompareAndSwap(sessionID, existingValue, fresh) {
		return
	}
	log.FromContext(ctx).V(logging.DEBUG).Info("Session affinity - session migrated pod",
		"plugin", string(s.pluginKey), "from", existing.podName, "to", podName)
}

// pickedPodName returns the pod chosen for this instance's profile, or "" when
// that profile was not scheduled. An empty profileName selects the primary
// (decode) profile.
func (s *SessionIDHeader) pickedPodName(schedulingResult *scheduling.SchedulingResult) string {
	if schedulingResult == nil {
		return ""
	}
	profileName := s.profileName
	if profileName == "" {
		profileName = schedulingResult.PrimaryProfileName
	}
	result, ok := schedulingResult.ProfileResults[profileName]
	if !ok || result == nil || len(result.TargetEndpoints) == 0 || result.TargetEndpoints[0] == nil {
		return ""
	}
	if md := result.TargetEndpoints[0].GetMetadata(); md != nil {
		return md.ID.String()
	}
	return ""
}

// ResponseHeader is a no-op: under session_id the client owns its
// session identifier.
func (s *SessionIDHeader) ResponseHeader(context.Context, *scheduling.InferenceRequest, *fwkrh.Response, *datalayer.EndpointMetadata) {
}

// expired reports whether a binding last used at lastSeen has outlived the TTL.
func (s *SessionIDHeader) expired(lastSeen, now time.Time) bool {
	return s.ttl > 0 && now.Sub(lastSeen) > s.ttl
}

// runEviction drops bindings unused for longer than the TTL until ctx is cancelled.
func (s *SessionIDHeader) runEviction(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.bindings.Range(func(key, value any) bool {
				b, ok := value.(binding)
				if !ok {
					s.mu.Lock()
					s.bindings.CompareAndDelete(key, value)
					s.mu.Unlock()
					return true
				}
				if !s.expired(b.lastSeen, now) {
					return true
				}
				// Re-check under the lock: a concurrent preRequest may have
				// refreshed this binding since Range snapshotted it, in which
				// case CompareAndDelete correctly declines to remove it.
				s.mu.Lock()
				s.bindings.CompareAndDelete(key, value)
				s.mu.Unlock()
				return true
			})
		}
	}
}
