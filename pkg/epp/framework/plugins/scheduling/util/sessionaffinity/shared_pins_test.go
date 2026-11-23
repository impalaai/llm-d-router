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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/test/utils"
)

// fakePinStore is an in-process CrossReplicaValueStore shared by test
// replicas. Each call is atomic, as each command is in Redis.
type fakePinStore struct {
	mu     sync.Mutex
	values map[string]string
	// err, when set, fails every call.
	err error
	// stall makes every call wait for its context to end.
	stall bool
	calls int
	ttls  []time.Duration
}

var _ fwkdl.CrossReplicaValueStore = (*fakePinStore)(nil)

func newFakePinStore() *fakePinStore {
	return &fakePinStore{values: map[string]string{}}
}

func (f *fakePinStore) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "fake-pin-store", Name: "fake-pin-store"}
}

// begin records a call and returns the error the call fails with, if any.
// The caller holds no lock.
func (f *fakePinStore) begin(ctx context.Context, ttl time.Duration) error {
	f.mu.Lock()
	f.calls++
	f.ttls = append(f.ttls, ttl)
	stall, err := f.stall, f.err
	f.mu.Unlock()
	if stall {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func (f *fakePinStore) LoadAndRefresh(ctx context.Context, key fwkdl.StateKey, id string, ttl time.Duration) (string, bool, error) {
	if err := f.begin(ctx, ttl); err != nil {
		return "", false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, found := f.values[string(key)+"/"+id]
	return value, found, nil
}

func (f *fakePinStore) LoadOrStore(ctx context.Context, key fwkdl.StateKey, id, value string, ttl time.Duration) (string, bool, error) {
	if err := f.begin(ctx, ttl); err != nil {
		return "", false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := string(key) + "/" + id
	if actual, found := f.values[k]; found {
		return actual, true, nil
	}
	f.values[k] = value
	return value, false, nil
}

func (f *fakePinStore) CompareAndSwap(ctx context.Context, key fwkdl.StateKey, id, old, value string, ttl time.Duration) (string, bool, error) {
	if err := f.begin(ctx, ttl); err != nil {
		return "", false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := string(key) + "/" + id
	if actual, found := f.values[k]; found && actual != old {
		return actual, false, nil
	}
	f.values[k] = value
	return value, true, nil
}

func (f *fakePinStore) clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values = map[string]string{}
}

func (f *fakePinStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakePinStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// pins returns the stored pod of every pin.
func (f *fakePinStore) pins() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	pods := make([]string, 0, len(f.values))
	for _, pod := range f.values {
		pods = append(pods, pod)
	}
	return pods
}

// pinReplica is one EPP replica: a SessionIDHeader on a shared store, with
// its own endpoint objects and request counter.
type pinReplica struct {
	t         *testing.T
	header    *SessionIDHeader
	endpoints map[string]scheduling.Endpoint
	requests  int
}

var testPods = []string{"pod-a", "pod-b", "pod-c"}

func newPinReplica(t *testing.T, syncer plugin.Plugin, configure func(*SessionIDConfig)) *pinReplica {
	t.Helper()
	return newNamedPinReplica(t, syncer, testTypedName, configure)
}

func newNamedPinReplica(t *testing.T, syncer plugin.Plugin, typedName plugin.TypedName, configure func(*SessionIDConfig)) *pinReplica {
	t.Helper()
	handle := utils.NewTestHandle(utils.NewTestContext(t))
	if syncer != nil {
		handle.SetCrossReplicaSyncer(syncer)
	}
	config := SessionIDConfig{Sources: []SessionIDSource{{Header: DefaultHeader}}}
	if configure != nil {
		configure(&config)
	}
	config.ApplyDefaults()
	require.NoError(t, config.Validate())
	r := &pinReplica{
		t:         t,
		header:    NewSessionIDHeader(config, "", typedName, handle),
		endpoints: map[string]scheduling.Endpoint{},
	}
	for _, pod := range testPods {
		r.endpoints[pod] = newTestEndpoint(pod)
	}
	return r
}

// pinRequest is one request of a session on one replica, between its
// scheduling and its routing.
type pinRequest struct {
	replica    *pinReplica
	request    *scheduling.InferenceRequest
	candidates []scheduling.Endpoint
	result     *scheduling.SchedulingResult
}

// choose starts a request for sessionID and returns it with the pod Choose
// pins it to among pods, or "" when Choose expresses no preference.
func (r *pinReplica) choose(sessionID string, pods ...string) (*pinRequest, string) {
	r.t.Helper()
	r.requests++
	request := requestWithSession(sessionID)
	request.RequestID += "-" + strings.Repeat("i", r.requests)
	candidates := make([]scheduling.Endpoint, 0, len(pods))
	for _, pod := range pods {
		candidates = append(candidates, r.endpoints[pod])
	}
	target, ok := r.header.Choose(context.Background(), request, candidates)
	pending := &pinRequest{replica: r, request: request, candidates: candidates}
	if !ok || target == nil {
		return pending, ""
	}
	return pending, target.GetMetadata().ID.Name
}

// revise passes the scheduler's pick of pod, with scored as the candidates the
// profile scored (all of the request's candidates when empty), through
// ReviseTargets and returns the pod the request is routed to.
func (p *pinRequest) revise(pod string, scored ...string) string {
	p.result = schedulingResultFor(p.replica.endpoints[pod])
	profile := p.result.ProfileResults[p.result.PrimaryProfileName]
	if len(scored) == 0 {
		for _, candidate := range p.candidates {
			profile.ScoredCandidates = append(profile.ScoredCandidates, scheduling.ScoredEndpoint{Endpoint: candidate})
		}
	}
	for _, name := range scored {
		profile.ScoredCandidates = append(profile.ScoredCandidates, scheduling.ScoredEndpoint{Endpoint: p.replica.endpoints[name]})
	}
	p.replica.header.ReviseTargets(context.Background(), p.request, p.result)
	return profile.TargetEndpoints[0].GetMetadata().ID.Name
}

// commit runs PreRequest on the result revise produced.
func (p *pinRequest) commit() {
	p.replica.header.PreRequest(context.Background(), p.request, p.result)
}

// route commits the request as routed to pod.
func (p *pinRequest) route(pod string) {
	p.replica.header.PreRequest(context.Background(), p.request, schedulingResultFor(p.replica.endpoints[pod]))
}

// serve runs one whole request for sessionID that the scheduler sends to pod
// unless Choose pins it elsewhere, and returns the pod it was routed to.
func (r *pinReplica) serve(sessionID, pod string, candidates ...string) string {
	r.t.Helper()
	if len(candidates) == 0 {
		candidates = testPods
	}
	request, pinned := r.choose(sessionID, candidates...)
	if pinned != "" {
		pod = pinned
	}
	request.route(pod)
	return pod
}

func TestSharedPins_PinIsHonouredByPeer(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)

	require.Equal(t, "pod-a", a.serve("s1", "pod-a"))
	require.Equal(t, "pod-a", b.serve("s1", "pod-c"))
}

func TestSharedPins_RawSessionIDIsNotStored(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, nil)
	a.serve("secret-session", "pod-a")

	require.Len(t, store.values, 1)
	for key := range store.values {
		require.NotContains(t, key, "secret-session")
	}
}

func TestSharedPins_RestartedReplicaFindsPin(t *testing.T) {
	store := newFakePinStore()
	newPinReplica(t, store, nil).serve("s1", "pod-b")

	restarted := newPinReplica(t, store, nil)
	require.Equal(t, "pod-b", restarted.serve("s1", "pod-c"))
}

// A new session reaches three replicas before any of them has routed it.
func TestSharedPins_NewSessionOnThreeReplicasConverges(t *testing.T) {
	store := newFakePinStore()
	replicas := []*pinReplica{newPinReplica(t, store, nil), newPinReplica(t, store, nil), newPinReplica(t, store, nil)}

	requests := make([]*pinRequest, len(replicas))
	for i, replica := range replicas {
		var pinned string
		requests[i], pinned = replica.choose("s1", testPods...)
		require.Empty(t, pinned)
	}
	for i, request := range requests {
		request.route(testPods[i])
	}

	require.Equal(t, []string{podKey("pod-a")}, store.pins(), "the first create wins")
	for _, replica := range replicas {
		require.Equal(t, podKey("pod-a"), replica.header.boundPod("s1"), "a replica that lost the create binds the winner locally")
		require.Equal(t, "pod-a", replica.serve("s1", "pod-c"))
	}
}

// The pinned pod leaves the pool and three replicas move the session at once.
func TestSharedPins_RepinAfterPodLossHasOneWinner(t *testing.T) {
	store := newFakePinStore()
	replicas := []*pinReplica{newPinReplica(t, store, nil), newPinReplica(t, store, nil), newPinReplica(t, store, nil)}
	for _, replica := range replicas {
		require.Equal(t, "pod-a", replica.serve("s1", "pod-a"))
	}

	requests := make([]*pinRequest, len(replicas))
	for i, replica := range replicas {
		var pinned string
		requests[i], pinned = replica.choose("s1", "pod-b", "pod-c")
		require.Empty(t, pinned)
	}
	requests[0].route("pod-b")
	requests[1].route("pod-c")
	requests[2].route("pod-c")

	require.Equal(t, []string{podKey("pod-b")}, store.pins(), "the first move wins")
	for _, replica := range replicas {
		require.Equal(t, podKey("pod-b"), replica.header.boundPod("s1"), "a replica that lost the move binds the winner locally")
		require.Equal(t, "pod-b", replica.serve("s1", "pod-c", "pod-b", "pod-c"))
	}
}

// The shared pin expired while one replica still holds it locally, and
// another replica pinned the session elsewhere.
func TestSharedPins_SharedPinOverridesStaleLocalPin(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")
	store.clear()
	require.Equal(t, "pod-b", b.serve("s1", "pod-b"))

	require.Equal(t, "pod-b", a.serve("s1", "pod-c"))
}

// One replica moves a live session; another still holds the old pin locally.
func TestSharedPins_MovedSessionOverridesLocalPin(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")
	b.serve("s1", "pod-a")
	require.Equal(t, "pod-b", a.serve("s1", "pod-b", "pod-b", "pod-c"))

	require.Equal(t, "pod-b", b.serve("s1", "pod-c"))
}

func TestSharedPins_PresentPinIsNotMovedWhenOutvoted(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")

	request, pinned := a.choose("s1", "pod-a", "pod-b")
	require.Equal(t, "pod-a", pinned)
	request.route("pod-b")

	require.Equal(t, []string{podKey("pod-a")}, store.pins())
	require.Equal(t, podKey("pod-a"), a.header.boundPod("s1"))
}

// The store lost its pins, for example in a failover. A replica that still
// holds the pin locally routes on it and stores it again.
func TestSharedPins_LocalPinRecreatesLostSharedPin(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")
	store.clear()

	require.Equal(t, "pod-a", a.serve("s1", "pod-c"))
	require.Equal(t, "pod-a", b.serve("s1", "pod-c"))
}

func TestSharedPins_StoreErrorFallsBackToLocalPins(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")

	store.setErr(errors.New("store down"))
	require.Equal(t, "pod-a", a.serve("s1", "pod-c"), "a known session routes on the local pin")
	require.Equal(t, "pod-b", a.serve("s2", "pod-b"), "a new session is scheduled normally")
	require.Equal(t, "pod-b", a.serve("s2", "pod-c"), "and pinned locally")

	store.setErr(nil)
	require.Equal(t, "pod-b", a.serve("s2", "pod-c"))
	require.ElementsMatch(t, []string{podKey("pod-a"), podKey("pod-b")}, store.pins(), "the local pin is stored once the store answers")
}

func TestSharedPins_StalledStoreReturnsByTheTimeout(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, func(c *SessionIDConfig) { c.StoreTimeoutSeconds = 0.02 })
	a.serve("s1", "pod-a")
	store.stall = true

	start := time.Now()
	require.Equal(t, "pod-a", a.serve("s1", "pod-c"))
	require.Less(t, time.Since(start), time.Second)
}

func TestSharedPins_ReadRefreshesTheEvictionTTL(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, func(c *SessionIDConfig) { c.EvictionTTLSeconds = 3600 })
	a.serve("s1", "pod-a")
	a.serve("s1", "pod-a")

	require.NotEmpty(t, store.ttls)
	for _, ttl := range store.ttls {
		require.Equal(t, time.Hour, ttl)
	}
	require.Equal(t, 3, store.callCount(), "a new session costs a read and a create, a known session one read")
}

func TestSharedPins_SyncDisabledKeepsPinsLocal(t *testing.T) {
	store := newFakePinStore()
	disabled := false
	a := newPinReplica(t, store, func(c *SessionIDConfig) { c.SyncCrossReplicaState = &disabled })
	b := newPinReplica(t, store, func(c *SessionIDConfig) { c.SyncCrossReplicaState = &disabled })

	require.Equal(t, "pod-a", a.serve("s1", "pod-a"))
	require.Equal(t, "pod-a", a.serve("s1", "pod-c"))
	require.Equal(t, "pod-c", b.serve("s1", "pod-c"))
	require.Zero(t, store.callCount())
}

// plainSyncer is a cross-replica syncer that cannot store values.
type plainSyncer struct{}

func (plainSyncer) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "plain-syncer", Name: "plain-syncer"}
}

func TestSharedPins_SyncerWithoutValueStoreKeepsPinsLocal(t *testing.T) {
	a := newPinReplica(t, plainSyncer{}, nil)
	require.Equal(t, "pod-a", a.serve("s1", "pod-a"))
	require.Equal(t, "pod-a", a.serve("s1", "pod-c"))
}

func TestSharedPins_NoSyncerKeepsPinsLocal(t *testing.T) {
	a := newPinReplica(t, nil, nil)
	require.Equal(t, "pod-a", a.serve("s1", "pod-a"))
	require.Equal(t, "pod-a", a.serve("s1", "pod-c"))
}

var metricsTestRuns atomic.Int64

func TestSharedPins_Metrics(t *testing.T) {
	store := newFakePinStore()
	// The collectors are process-wide, so each run needs its own label values.
	name := plugin.TypedName{Type: "session-affinity-test", Name: t.Name() + "-" + strconv.FormatInt(metricsTestRuns.Add(1), 10)}
	a, b := newNamedPinReplica(t, store, name, nil), newNamedPinReplica(t, store, name, nil)

	a.serve("s1", "pod-a")
	b.serve("s1", "pod-c")

	first, _ := a.choose("s2", testPods...)
	second, _ := b.choose("s2", testPods...)
	first.route("pod-a")
	second.route("pod-b")

	first, _ = a.choose("s1", "pod-b", "pod-c")
	second, _ = b.choose("s1", "pod-b", "pod-c")
	first.route("pod-b")
	second.route("pod-c")

	store.setErr(errors.New("store down"))
	a.serve("s1", "pod-b")

	want := map[string]float64{
		pinResultHit:        3,
		pinResultMiss:       3,
		pinResultCreated:    2,
		pinResultCreateLost: 1,
		pinResultMoved:      1,
		pinResultMoveLost:   1,
		pinResultError:      1,
	}
	for result, count := range want {
		require.Equal(t, count, testutil.ToFloat64(sharedPinResults.WithLabelValues(name.Name, name.Type, result)), result)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(sharedPinCallDuration)
	families, err := registry.Gather()
	require.NoError(t, err)
	calls := map[string]uint64{}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["plugin_name"] == name.Name {
				calls[labels["operation"]] = metric.GetHistogram().GetSampleCount()
			}
		}
	}
	require.Equal(t, map[string]uint64{pinOperationRead: 7, pinOperationCreate: 3, pinOperationMove: 2}, calls)
}

// Two replicas schedule a new session at once. The replica that loses the
// create routes its request to the pod the other replica pinned.
func TestSharedPins_LosingCreateIsRoutedToTheWinner(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	first, _ := a.choose("s1", testPods...)
	second, _ := b.choose("s1", testPods...)

	require.Equal(t, "pod-a", first.revise("pod-a"))
	require.Equal(t, "pod-a", second.revise("pod-b"))
	require.Same(t, b.endpoints["pod-a"], second.result.ProfileResults["default"].TargetEndpoints[0])

	calls := store.callCount()
	first.commit()
	second.commit()
	require.Equal(t, calls, store.callCount(), "PreRequest does not write a pin ReviseTargets settled")
	require.Equal(t, podKey("pod-a"), b.header.boundPod("s1"))
}

func TestSharedPins_LosingMoveIsRoutedToTheWinner(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	a.serve("s1", "pod-a")
	first, _ := a.choose("s1", "pod-b", "pod-c")
	second, _ := b.choose("s1", "pod-b", "pod-c")

	require.Equal(t, "pod-b", first.revise("pod-b"))
	require.Equal(t, "pod-b", second.revise("pod-c"))
}

// The winning pod was a candidate of the request but the profile did not
// score it, for example because a later filter removed it.
func TestSharedPins_WinnerOutsideTheScoredCandidatesIsNotRoutedTo(t *testing.T) {
	store := newFakePinStore()
	a, b := newPinReplica(t, store, nil), newPinReplica(t, store, nil)
	first, _ := a.choose("s1", testPods...)
	second, _ := b.choose("s1", testPods...)
	first.revise("pod-a")

	require.Equal(t, "pod-b", second.revise("pod-b", "pod-b", "pod-c"))
	second.commit()
	require.Equal(t, podKey("pod-a"), b.header.boundPod("s1"), "the stored pin is still bound locally")
}

func TestSharedPins_ReviseTargetsKeepsThePickWhenTheStoreFails(t *testing.T) {
	store := newFakePinStore()
	a := newPinReplica(t, store, nil)
	request, _ := a.choose("s1", testPods...)
	store.setErr(errors.New("store down"))

	require.Equal(t, "pod-b", request.revise("pod-b"))
	request.commit()
	require.Equal(t, podKey("pod-b"), a.header.boundPod("s1"))
}

func TestSharedPins_ReviseTargetsLeavesAnUnsharedRequestAlone(t *testing.T) {
	a := newPinReplica(t, nil, nil)
	request, _ := a.choose("s1", testPods...)
	require.Equal(t, "pod-b", request.revise("pod-b"))
}
