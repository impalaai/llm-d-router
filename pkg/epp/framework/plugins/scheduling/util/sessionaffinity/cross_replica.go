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
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/cespare/xxhash/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
)

const (
	// sessionBindingsDataType names the endpoint attribute that carries the
	// session bindings shared across EPP replicas.
	sessionBindingsDataType = "SessionBindings"
	// defaultMaxSharedSessionsPerEndpoint bounds one replica's published view
	// of an endpoint to about 180KB once gob-encoded.
	defaultMaxSharedSessionsPerEndpoint = 10000
)

var (
	_ datalayer.EndpointExtractor       = (*SessionIDHeader)(nil)
	_ datalayer.Registrant              = (*SessionIDHeader)(nil)
	_ datalayer.CrossReplicaContributor = (*SessionIDHeader)(nil)
	_ datalayer.CrossReplicaPreparer    = (*SessionIDHeader)(nil)
)

// sessionBindings is the shared view of the sessions bound to one endpoint.
// Each view maps sessionHash to the binding's last use in Unix nanoseconds.
// Read publishes one view per replica and Aggregate concatenates them, so a
// session present in several views resolves to its newest lastSeen at lookup.
// Views are never written once built, so Clone and Aggregate share them
// rather than copying every binding on each attribute read.
type sessionBindings struct {
	Views []map[uint64]int64
}

// Clone copies the view list and shares the immutable views.
func (b *sessionBindings) Clone() datalayer.Cloneable {
	if b == nil {
		return nil
	}
	return &sessionBindings{Views: slices.Clone(b.Views)}
}

// lastSeen returns the newest lastSeen recorded for hash across all views.
func (b *sessionBindings) lastSeen(hash uint64) (int64, bool) {
	var newest int64
	found := false
	for _, view := range b.Views {
		if lastSeen, ok := view[hash]; ok && (!found || lastSeen > newest) {
			newest, found = lastSeen, true
		}
	}
	return newest, found
}

func aggregateSessionBindings(values []any) any {
	out := &sessionBindings{}
	for _, value := range values {
		if b, ok := value.(*sessionBindings); ok && b != nil {
			out.Views = append(out.Views, b.Views...)
		}
	}
	return out
}

// sessionHash keys a session in shared state so that session identifiers do
// not leave the process.
func sessionHash(sessionID string) uint64 {
	return xxhash.Sum64String(sessionID)
}

// RegisterDependencies registers this strategy as an endpoint extractor so
// the cross-replica publisher finds it as a contributor. With
// syncCrossReplicaState off, or without a configured syncer, it registers
// nothing and bindings stay local to this replica.
func (s *SessionIDHeader) RegisterDependencies(r datalayer.Registrar) error {
	if !s.syncCrossReplicaState || s.handle.CrossReplicaSyncer() == nil {
		return nil
	}
	if err := r.Register(datalayer.PendingRegistration{
		Owner:         s.typedName,
		SourceType:    sourcenotifications.EndpointNotificationSourceType,
		Extractor:     s,
		DefaultSource: sourcenotifications.NewEndpointDataSource(sourcenotifications.EndpointNotificationSourceType, sourcenotifications.EndpointNotificationSourceType),
	}); err != nil {
		return err
	}
	s.syncActive.Store(true)
	return nil
}

// Extract does nothing. The cross-replica publisher installs the bindings
// attribute on each endpoint this extractor is registered for.
func (s *SessionIDHeader) Extract(context.Context, datalayer.EndpointEvent) error {
	return nil
}

// CrossReplicaState shares the session bindings of each endpoint with peer
// EPP replicas.
func (s *SessionIDHeader) CrossReplicaState() datalayer.CrossReplicaSpec {
	return datalayer.CrossReplicaSpec{
		StateKey:     datalayer.StateKey("session-affinity:" + s.typedName.Name),
		AttributeKey: s.bindingsKey,
		SyncDisabled: !s.syncCrossReplicaState,
		Read: func(endpointID string) datalayer.Cloneable {
			if views := s.sharedViews.Load(); views != nil {
				if view, ok := (*views)[endpointID]; ok {
					return &sessionBindings{Views: []map[uint64]int64{view}}
				}
			}
			return &sessionBindings{}
		},
		Aggregate: aggregateSessionBindings,
	}
}

// PrepareCrossReplicaState snapshots the unexpired bindings, grouped by
// endpoint, as the views Read publishes, keeping the maxSharedSessions most
// recently used per endpoint. Building them once per publication cycle keeps
// the request path free of publication work.
func (s *SessionIDHeader) PrepareCrossReplicaState() {
	now := time.Now()
	views := map[string]map[uint64]int64{}
	s.bindings.Range(func(key, value any) bool {
		sessionID, ok := key.(string)
		b, isBinding := value.(binding)
		if !ok || !isBinding || s.expired(b.lastSeen, now) {
			return true
		}
		view := views[b.podName]
		if view == nil {
			view = map[uint64]int64{}
			views[b.podName] = view
		}
		view[sessionHash(sessionID)] = b.lastSeen.UnixNano()
		return true
	})
	for pod, view := range views {
		if len(view) > s.maxSharedSessions {
			views[pod] = newestSessions(view, s.maxSharedSessions)
		}
	}
	s.sharedViews.Store(&views)
}

// newestSessions returns the n entries of view with the latest lastSeen.
func newestSessions(view map[uint64]int64, n int) map[uint64]int64 {
	hashes := slices.SortedFunc(maps.Keys(view), func(a, b uint64) int {
		return cmp.Compare(view[b], view[a])
	})
	newest := make(map[uint64]int64, n)
	for _, hash := range hashes[:n] {
		newest[hash] = view[hash]
	}
	return newest
}

// adoptNewestBinding replaces the local binding of sessionID with the newest
// unexpired binding any replica holds on a candidate endpoint, when that
// binding was used more recently. A binding known only to a peer replica is
// adopted the same way, so this replica honours and republishes it.
func (s *SessionIDHeader) adoptNewestBinding(ctx context.Context, sessionID string, endpoints []scheduling.Endpoint) {
	hash := sessionHash(sessionID)
	now := time.Now()
	var newestPod string
	var newest int64
	for _, endpoint := range endpoints {
		value, ok := endpoint.Get(s.bindingsKey)
		if !ok {
			continue
		}
		bindings, ok := value.(*sessionBindings)
		if !ok {
			continue
		}
		lastSeen, ok := bindings.lastSeen(hash)
		if !ok || lastSeen <= newest || s.expired(time.Unix(0, lastSeen), now) {
			continue
		}
		newestPod, newest = endpoint.GetMetadata().ID.String(), lastSeen
	}
	if newestPod == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var fromPod string
	if existingValue, loaded := s.bindings.Load(sessionID); loaded {
		existing, ok := existingValue.(binding)
		if !ok || existing.lastSeen.UnixNano() >= newest {
			return
		}
		fromPod = existing.podName
	}
	s.bindings.Store(sessionID, binding{podName: newestPod, lastSeen: time.Unix(0, newest)})
	log.FromContext(ctx).V(logging.DEBUG).Info("Session affinity - adopted binding from a peer replica",
		"plugin", string(s.pluginKey), "pod", newestPod, "from", fromPod)
}
