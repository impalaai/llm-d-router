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
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/test/utils"
)

// memorySyncer is an in-process stand-in for redis-syncer shared by test
// replicas. Like redis-syncer it gob-encodes each published value, decodes peer
// values into the type of the local value, and Get combines the live local
// value with the aggregate of the other replicas' values.
type memorySyncer struct {
	mu sync.Mutex
	// fields maps state key and endpoint ID to replica ID to encoded value.
	fields map[string]map[string][]byte
}

func newMemorySyncer() *memorySyncer {
	return &memorySyncer{fields: map[string]map[string][]byte{}}
}

// forget drops everything a replica published, as stateTTL expiry does.
func (m *memorySyncer) forget(replicaID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, replicas := range m.fields {
		delete(replicas, replicaID)
	}
}

// replicaSyncer is one replica's handle on a memorySyncer.
type replicaSyncer struct {
	store     *memorySyncer
	replicaID string
}

var _ fwkdl.CrossReplicaSyncer = (*replicaSyncer)(nil)

func (r *replicaSyncer) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "memory-syncer", Name: r.replicaID}
}

func (r *replicaSyncer) Set(_ context.Context, spec fwkdl.CrossReplicaSpec, endpointID string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(spec.Read(endpointID)); err != nil {
		return err
	}
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	key := string(spec.StateKey) + ":" + endpointID
	if r.store.fields[key] == nil {
		r.store.fields[key] = map[string][]byte{}
	}
	r.store.fields[key][r.replicaID] = buf.Bytes()
	return nil
}

func (r *replicaSyncer) Get(_ context.Context, spec fwkdl.CrossReplicaSpec, endpointID string) (any, bool, error) {
	local := spec.Read(endpointID)
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	var peers []any
	for replicaID, data := range r.store.fields[string(spec.StateKey)+":"+endpointID] {
		if replicaID == r.replicaID {
			continue
		}
		target := reflect.New(reflect.TypeOf(local))
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(target.Interface()); err != nil {
			return nil, false, err
		}
		peers = append(peers, target.Elem().Interface())
	}
	if len(peers) == 0 {
		return local, true, nil
	}
	return spec.Aggregate([]any{local, spec.Aggregate(peers)}), true, nil
}

func (r *replicaSyncer) Delete(context.Context, fwkdl.StateKey, string) error {
	return nil
}

func (r *replicaSyncer) GetOrSet(_ context.Context, _ fwkdl.StateKey, _ string, candidate any) (any, bool, error) {
	return candidate, false, nil
}

// recordingRegistrar captures the datalayer registrations a plugin declares.
type recordingRegistrar struct {
	registrations []fwkdl.PendingRegistration
}

func (r *recordingRegistrar) Register(reg fwkdl.PendingRegistration) error {
	r.registrations = append(r.registrations, reg)
	return nil
}

// testReplica is one EPP replica: a SessionIDHeader registered against a
// shared syncer, with its own endpoint objects. When the header registers as a
// contributor, each endpoint carries the attribute the cross-replica publisher
// installs.
type testReplica struct {
	t         *testing.T
	header    *SessionIDHeader
	syncer    *replicaSyncer
	registrar *recordingRegistrar
	endpoints map[string]scheduling.Endpoint
}

var testPods = []string{"pod-a", "pod-b", "pod-c"}

func newTestReplica(t *testing.T, store *memorySyncer, replicaID string, syncCrossReplicaState *bool) *testReplica {
	t.Helper()
	handle := utils.NewTestHandle(utils.NewTestContext(t))
	syncer := &replicaSyncer{store: store, replicaID: replicaID}
	handle.SetCrossReplicaSyncer(syncer)
	header := NewSessionIDHeader(SessionIDConfig{
		Sources:               []SessionIDSource{{Header: DefaultHeader}},
		EvictionTTLSeconds:    300,
		EvictionSweepSeconds:  10,
		SyncCrossReplicaState: syncCrossReplicaState,
	}, "", testTypedName, handle)

	registrar := &recordingRegistrar{}
	if err := header.RegisterDependencies(registrar); err != nil {
		t.Fatalf("RegisterDependencies: %v", err)
	}

	r := &testReplica{
		t:         t,
		header:    header,
		syncer:    syncer,
		registrar: registrar,
		endpoints: map[string]scheduling.Endpoint{},
	}
	spec := header.CrossReplicaState()
	for _, pod := range testPods {
		endpoint := scheduling.NewEndpoint(
			&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: pod}},
			&fwkdl.Metrics{},
			fwkdl.NewAttributes(),
		)
		if len(registrar.registrations) > 0 && !spec.SyncDisabled {
			endpointID := endpoint.GetMetadata().ID.String()
			endpoint.Put(spec.AttributeKey, &fwkdl.DynamicAttribute{
				Get: func() fwkdl.Cloneable {
					value, ok, err := syncer.Get(context.Background(), spec, endpointID)
					if err != nil || !ok {
						return spec.Read(endpointID)
					}
					return value.(fwkdl.Cloneable)
				},
			})
		}
		r.endpoints[pod] = endpoint
	}
	return r
}

// publish runs one cross-replica publication cycle over every endpoint, as
// the publisher does.
func (r *testReplica) publish() {
	r.t.Helper()
	r.header.PrepareCrossReplicaState()
	spec := r.header.CrossReplicaState()
	for _, endpoint := range r.endpoints {
		if err := r.syncer.Set(context.Background(), spec, endpoint.GetMetadata().ID.String()); err != nil {
			r.t.Fatalf("Set: %v", err)
		}
	}
}

// choose returns the pod Choose pins sessionID to among pods, or "" when it
// expresses no preference.
func (r *testReplica) choose(sessionID string, pods ...string) string {
	target, ok := r.header.Choose(context.Background(), requestWithSession(sessionID), r.candidates(pods...))
	if !ok || target == nil {
		return ""
	}
	return target.GetMetadata().ID.Name
}

func (r *testReplica) candidates(pods ...string) []scheduling.Endpoint {
	candidates := make([]scheduling.Endpoint, 0, len(pods))
	for _, pod := range pods {
		candidates = append(candidates, r.endpoints[pod])
	}
	return candidates
}

// route commits a request for sessionID that was routed to pod.
func (r *testReplica) route(sessionID, pod string) {
	r.header.PreRequest(context.Background(), requestWithSession(sessionID), schedulingResultFor(r.endpoints[pod]))
}

// TestCrossReplica_PinIsHonouredByPeer checks that bindings made on one
// replica route each session's next request on another replica.
func TestCrossReplica_PinIsHonouredByPeer(t *testing.T) {
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)

	if len(a.registrar.registrations) != 1 {
		t.Fatalf("registrations = %d, want 1: an enabled strategy with a syncer must register as a contributor", len(a.registrar.registrations))
	}
	if reg := a.registrar.registrations[0]; reg.Extractor != plugin.Plugin(a.header) || reg.Owner != testTypedName {
		t.Errorf("registration = %+v, want the header registered under its owner's typed name", reg)
	}

	a.route("s1", "pod-b")
	a.route("s2", "pod-a")
	a.publish()

	if got := b.choose("s1", testPods...); got != "pod-b" {
		t.Errorf("peer Choose for s1 = %q, want pod-b", got)
	}
	if got := b.choose("s2", testPods...); got != "pod-a" {
		t.Errorf("peer Choose for s2 = %q, want pod-a", got)
	}
}

// TestCrossReplica_PeerBindingReadThroughScope checks that a replica reads
// peer bindings through the scoped endpoints filters and scorers receive.
func TestCrossReplica_PeerBindingReadThroughScope(t *testing.T) {
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)

	a.route("s1", "pod-b")
	a.publish()

	datalayer.RegisterScopeSpecs([]plugin.Plugin{b.header})
	scoped, _ := datalayer.Scope(logr.Discard(), "Score", b.header,
		[]scheduling.Endpoint{b.endpoints["pod-a"], b.endpoints["pod-b"]})
	target, ok := b.header.Choose(context.Background(), requestWithSession("s1"), scoped)
	if !ok || target == nil || target.GetMetadata().ID.Name != "pod-b" {
		t.Errorf("Choose through scoped endpoints = (%v, %v), want (pod-b, true)", target, ok)
	}
}

// TestCrossReplica_RawSessionIDIsNotPublished checks that published state
// carries a hash of the session identifier, never the identifier itself.
func TestCrossReplica_RawSessionIDIsNotPublished(t *testing.T) {
	const sessionID = "conversation-7f3a9c41d2"
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)

	a.route(sessionID, "pod-a")
	a.publish()

	published := 0
	for key, replicas := range store.fields {
		for replicaID, data := range replicas {
			published++
			if bytes.Contains(data, []byte(sessionID)) {
				t.Errorf("value published by %s under %s contains the raw session identifier", replicaID, key)
			}
		}
	}
	if published == 0 {
		t.Fatal("nothing was published")
	}
}

// TestCrossReplica_RestartRestoresPins checks that a fresh replica honours the
// bindings a terminated replica published, adopts them, and republishes them
// once the terminated replica's state has expired.
func TestCrossReplica_RestartRestoresPins(t *testing.T) {
	store := newMemorySyncer()
	old := newTestReplica(t, store, "old", nil)
	old.route("s1", "pod-b")
	old.publish()

	restarted := newTestReplica(t, store, "restarted", nil)
	if got := restarted.choose("s1", testPods...); got != "pod-b" {
		t.Fatalf("restarted Choose = %q, want pod-b from the terminated replica's state", got)
	}
	if got, want := restarted.header.boundPod("s1"), podKey("pod-b"); got != want {
		t.Errorf("restarted boundPod = %q, want %q: a peer-only pin must be adopted locally", got, want)
	}

	restarted.publish()
	store.forget("old")

	next := newTestReplica(t, store, "next", nil)
	if got := next.choose("s1", testPods...); got != "pod-b" {
		t.Errorf("next Choose = %q, want pod-b republished by the restarted replica", got)
	}
}

// TestCrossReplica_NewestBindingWins checks that replicas holding conflicting
// bindings for one session converge on the most recently used one.
func TestCrossReplica_NewestBindingWins(t *testing.T) {
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)

	a.route("s1", "pod-a")
	time.Sleep(time.Millisecond)
	b.route("s1", "pod-b")
	a.publish()
	b.publish()

	if got := a.choose("s1", testPods...); got != "pod-b" {
		t.Errorf("replica a Choose = %q, want pod-b, the newer binding", got)
	}
	if got, want := a.header.boundPod("s1"), podKey("pod-b"); got != want {
		t.Errorf("replica a boundPod = %q, want %q: the newer peer binding replaces the older local one", got, want)
	}
	if got := b.choose("s1", testPods...); got != "pod-b" {
		t.Errorf("replica b Choose = %q, want pod-b, its own newer binding", got)
	}
}

// TestCrossReplica_RebindPropagates checks that a session migrated on one
// replica follows the migration on a peer that had adopted the old binding.
func TestCrossReplica_RebindPropagates(t *testing.T) {
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)

	a.route("s1", "pod-a")
	a.publish()
	if got := b.choose("s1", testPods...); got != "pod-a" {
		t.Fatalf("replica b Choose = %q, want pod-a", got)
	}
	b.route("s1", "pod-a")
	b.publish()

	// pod-a leaves replica a's candidate set, so a migrates the session.
	time.Sleep(time.Millisecond)
	if got := a.choose("s1", "pod-b"); got != "" {
		t.Fatalf("replica a Choose without pod-a = %q, want no preference", got)
	}
	a.route("s1", "pod-b")
	a.publish()

	if got := b.choose("s1", testPods...); got != "pod-b" {
		t.Errorf("replica b Choose = %q, want pod-b after the rebind on replica a", got)
	}
	if got, want := b.header.boundPod("s1"), podKey("pod-b"); got != want {
		t.Errorf("replica b boundPod = %q, want %q", got, want)
	}
}

// TestCrossReplica_AdoptionSurvivesStaleAbsence checks that a request whose
// Choose recorded the session's earlier pod as absent does not migrate the
// session after a concurrent request adopted a newer binding from a peer.
func TestCrossReplica_AdoptionSurvivesStaleAbsence(t *testing.T) {
	ctx := context.Background()
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)
	request := func(requestID string) *scheduling.InferenceRequest {
		return &scheduling.InferenceRequest{RequestID: requestID, Headers: map[string]string{DefaultHeader: "s1"}}
	}

	b.route("s1", "pod-a")
	first := request("first")
	if target, ok := b.header.Choose(ctx, first, b.candidates("pod-b", "pod-c")); ok {
		t.Fatalf("first Choose = %v, want no preference while pod-a is absent", target)
	}

	time.Sleep(time.Millisecond)
	a.route("s1", "pod-b")
	a.publish()
	if target, ok := b.header.Choose(ctx, request("second"), b.candidates(testPods...)); !ok || target.GetMetadata().ID.Name != "pod-b" {
		t.Fatalf("second Choose = (%v, %v), want (pod-b, true) from the newer peer binding", target, ok)
	}

	b.header.PreRequest(ctx, first, schedulingResultFor(b.endpoints["pod-c"]))
	if got, want := b.header.boundPod("s1"), podKey("pod-b"); got != want {
		t.Errorf("boundPod = %q, want %q: an absence recorded for pod-a must not move a session adopted onto pod-b", got, want)
	}
}

// TestCrossReplica_ExpiredBindingsAreNotPublished checks that a binding past
// its TTL is left out of the published state, and that a peer does not honour
// one it still receives, as from a stopped replica's retained state.
func TestCrossReplica_ExpiredBindingsAreNotPublished(t *testing.T) {
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", nil)
	b := newTestReplica(t, store, "b", nil)
	expired := time.Now().Add(-time.Hour)

	a.header.bindings.Store("s1", binding{podName: podKey("pod-b"), lastSeen: expired})
	a.publish()
	value, ok := a.header.CrossReplicaState().Read(podKey("pod-b")).(*sessionBindings)
	if !ok {
		t.Fatalf("Read returned %T, want *sessionBindings", value)
	}
	if _, found := value.lastSeen(sessionHash("s1")); found {
		t.Error("expired binding is published")
	}

	var buf bytes.Buffer
	stale := &sessionBindings{Views: []map[uint64]int64{{sessionHash("s1"): expired.UnixNano()}}}
	if err := gob.NewEncoder(&buf).Encode(stale); err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.fields[string(a.header.CrossReplicaState().StateKey)+":"+podKey("pod-b")] = map[string][]byte{"stopped": buf.Bytes()}
	if got := b.choose("s1", testPods...); got != "" {
		t.Errorf("peer Choose = %q, want no preference for an expired peer binding", got)
	}
}

// TestCrossReplica_ReadServesPreparedViews checks that Read returns the
// bindings as of the last PrepareCrossReplicaState, so a binding is published
// in the first cycle prepared after it is made.
func TestCrossReplica_ReadServesPreparedViews(t *testing.T) {
	a := newTestReplica(t, newMemorySyncer(), "a", nil)
	read := a.header.CrossReplicaState().Read
	published := func() bool {
		t.Helper()
		value, ok := read(podKey("pod-b")).(*sessionBindings)
		if !ok {
			t.Fatalf("Read returned %T, want *sessionBindings", value)
		}
		_, found := value.lastSeen(sessionHash("s1"))
		return found
	}

	a.route("s1", "pod-b")
	if published() {
		t.Error("binding published before a cycle was prepared")
	}
	a.header.PrepareCrossReplicaState()
	if !published() {
		t.Error("binding not published after a cycle was prepared")
	}
}

// TestCrossReplica_PublishesNewestSessionsUpToCap checks that each endpoint's
// published view keeps only the most recently used sessions up to the cap,
// while every binding keeps routing locally.
func TestCrossReplica_PublishesNewestSessionsUpToCap(t *testing.T) {
	a := newTestReplica(t, newMemorySyncer(), "a", nil)
	if a.header.maxSharedSessions != defaultMaxSharedSessionsPerEndpoint {
		t.Errorf("maxSharedSessions = %d, want the default %d when unset", a.header.maxSharedSessions, defaultMaxSharedSessionsPerEndpoint)
	}
	a.header.maxSharedSessions = 2
	now := time.Now()
	for i, sessionID := range []string{"s1", "s2", "s3"} {
		a.header.bindings.Store(sessionID, binding{podName: podKey("pod-a"), lastSeen: now.Add(time.Duration(i) * time.Second)})
	}
	a.header.PrepareCrossReplicaState()

	value, ok := a.header.CrossReplicaState().Read(podKey("pod-a")).(*sessionBindings)
	if !ok {
		t.Fatalf("Read returned %T, want *sessionBindings", value)
	}
	for sessionID, want := range map[string]bool{"s1": false, "s2": true, "s3": true} {
		if _, found := value.lastSeen(sessionHash(sessionID)); found != want {
			t.Errorf("%s published = %v, want %v", sessionID, found, want)
		}
	}
	if got := a.choose("s1", testPods...); got != "pod-a" {
		t.Errorf("local Choose for unpublished s1 = %q, want pod-a", got)
	}
}

// TestCrossReplica_SyncDisabledKeepsBindingsLocal checks that
// syncCrossReplicaState=false behaves as a replica without sharing.
func TestCrossReplica_SyncDisabledKeepsBindingsLocal(t *testing.T) {
	off := false
	store := newMemorySyncer()
	a := newTestReplica(t, store, "a", &off)
	b := newTestReplica(t, store, "b", &off)

	if len(a.registrar.registrations) != 0 {
		t.Errorf("registrations = %d, want 0 with syncCrossReplicaState=false", len(a.registrar.registrations))
	}
	if !a.header.CrossReplicaState().SyncDisabled {
		t.Error("SyncDisabled = false, want true with syncCrossReplicaState=false")
	}

	a.route("s1", "pod-b")
	if got := b.choose("s1", testPods...); got != "" {
		t.Errorf("peer Choose = %q, want no preference", got)
	}
}

// TestCrossReplica_NoSyncerDoesNoContributorWork checks that without a
// configured syncer the strategy registers nothing and still routes on local
// bindings.
func TestCrossReplica_NoSyncerDoesNoContributorWork(t *testing.T) {
	s := newTestSessionIDHeader(t, nil)
	registrar := &recordingRegistrar{}
	if err := s.RegisterDependencies(registrar); err != nil {
		t.Fatalf("RegisterDependencies: %v", err)
	}
	if len(registrar.registrations) != 0 {
		t.Errorf("registrations = %d, want 0 without a syncer", len(registrar.registrations))
	}

	endpointA := newTestEndpoint("pod-a")
	endpointB := newTestEndpoint("pod-b")
	s.PreRequest(context.Background(), requestWithSession("s1"), schedulingResultFor(endpointB))
	if got, ok := s.Choose(context.Background(), requestWithSession("s1"), []scheduling.Endpoint{endpointA, endpointB}); !ok || got != endpointB {
		t.Errorf("Choose = (%v, %v), want (pod-b, true)", got, ok)
	}
	if got := s.Produces(); got != nil {
		t.Errorf("Produces = %v, want nil without a syncer", got)
	}
}
