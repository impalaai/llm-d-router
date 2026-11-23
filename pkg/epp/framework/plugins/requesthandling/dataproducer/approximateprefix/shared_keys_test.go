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

package approximateprefix

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkrequest "github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// fakeSetStore is an in-memory CrossReplicaSetStore that several producers
// share, standing in for one Redis.
type fakeSetStore struct {
	mu      sync.Mutex
	sets    map[string]map[string]struct{}
	addIDs  [][]string
	loadIDs [][]string
	removes int
	err     error
	stall   bool
	// gate, when set, holds every AddToSets until it is closed; entered
	// receives once a held write has started.
	gate    chan struct{}
	entered chan struct{}
}

func newFakeSetStore() *fakeSetStore {
	return &fakeSetStore{sets: map[string]map[string]struct{}{}}
}

func (*fakeSetStore) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "fake-set-store", Name: "test"}
}

func (f *fakeSetStore) fail(ctx context.Context) error {
	f.mu.Lock()
	err, stall := f.err, f.stall
	f.mu.Unlock()
	if stall {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func (f *fakeSetStore) AddToSets(ctx context.Context, key fwkdl.StateKey, ids []string, member string, _ time.Duration) error {
	if err := f.fail(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	gate, entered := f.gate, f.entered
	f.mu.Unlock()
	if gate != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addIDs = append(f.addIDs, ids)
	for _, id := range ids {
		k := string(key) + "/" + id
		if f.sets[k] == nil {
			f.sets[k] = map[string]struct{}{}
		}
		f.sets[k][member] = struct{}{}
	}
	return nil
}

func (f *fakeSetStore) RemoveFromSets(ctx context.Context, key fwkdl.StateKey, ids []string, member string) error {
	if err := f.fail(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes++
	for _, id := range ids {
		k := string(key) + "/" + id
		delete(f.sets[k], member)
		if len(f.sets[k]) == 0 {
			delete(f.sets, k)
		}
	}
	return nil
}

func (f *fakeSetStore) LoadSets(ctx context.Context, key fwkdl.StateKey, ids []string) ([][]string, error) {
	if err := f.fail(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadIDs = append(f.loadIDs, ids)
	out := make([][]string, len(ids))
	for i, id := range ids {
		for member := range f.sets[string(key)+"/"+id] {
			out[i] = append(out[i], member)
		}
	}
	return out, nil
}

func (f *fakeSetStore) counts() (adds, loads, keys int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.addIDs), len(f.loadIDs), len(f.sets)
}

func sharedKeysConfig() config {
	return config{
		BlockSizeTokens:        1,
		MaxPrefixBlocksToMatch: 4096,
		LRUCapacityPerServer:   defaultLRUCapacityPerServer,
		SharedPrefixKeys:       &sharedPrefixKeysConfig{},
	}
}

// newSharingProducer returns a producer, standing in for one EPP, that shares
// prefix keys through store.
func newSharingProducer(t *testing.T, store *fakeSetStore) *dataProducer {
	t.Helper()
	disableMinBlockSizeClamp(t)
	h := plugin.NewEppHandle(context.Background(), nil, plugin.WithMetricsRecorder(prometheus.NewRegistry()))
	h.SetCrossReplicaSyncer(store)
	p, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, sharedKeysConfig(), h)
	require.NoError(t, err)
	return p
}

func testEndpoint(name string) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: name, Namespace: "default"}}, fwkdl.NewMetrics(), fwkdl.NewAttributes())
}

func tokens(n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i + 1)
	}
	return out
}

// route runs Produce and PreRequest for a prompt and sends it to target.
func route(t *testing.T, p *dataProducer, pods []fwksched.Endpoint, target fwksched.Endpoint, tokenIDs []uint32) *fwksched.InferenceRequest {
	t.Helper()
	req := &fwksched.InferenceRequest{RequestID: "req-" + target.GetMetadata().ID.Name + "-" + time.Now().String(), TargetModel: "m", Body: tokenizedBody(tokenIDs)}
	require.NoError(t, p.Produce(context.Background(), req, pods))
	res := &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults:     map[string]*fwksched.ProfileRunResult{"default": {TargetEndpoints: []fwksched.Endpoint{target}}},
	}
	require.NoError(t, p.PreRequest(context.Background(), req, res))
	p.wg.Wait()
	return req
}

// matched runs Produce for a prompt and returns the blocks matched per pod.
func matched(t *testing.T, p *dataProducer, pods []fwksched.Endpoint, tokenIDs []uint32) []int {
	t.Helper()
	req := &fwksched.InferenceRequest{RequestID: "probe-" + time.Now().String(), TargetModel: "m", Body: tokenizedBody(tokenIDs)}
	require.NoError(t, p.Produce(context.Background(), req, pods))
	p.pluginState.Delete(req.RequestID)
	out := make([]int, len(pods))
	for i, pod := range pods {
		info, ok := pod.Get(p.dk)
		require.True(t, ok)
		out[i] = info.(*attrprefix.PrefixCacheMatchInfo).MatchBlocks()
	}
	return out
}

func TestGridPositions(t *testing.T) {
	require.Equal(t, []int{8, 16, 20}, gridPositions(20, 8, 0))
	require.Equal(t, []int{8, 16}, gridPositions(16, 8, 0))
	require.Equal(t, []int{3}, gridPositions(3, 8, 0))
	require.Empty(t, gridPositions(0, 8, 0))
	require.Equal(t, []int{16, 20}, gridPositions(20, 8, 8), "positions up to the known run are left out")
	require.Empty(t, gridPositions(20, 8, 20))
}

func TestSharedKeysPublishGridKeysOnly(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	e1 := testEndpoint("e1")

	route(t, p, []fwksched.Endpoint{e1}, e1, tokens(20))

	adds, _, keys := store.counts()
	require.Equal(t, 1, adds, "one batched write per routed prompt")
	require.Equal(t, 3, keys, "blocks 8, 16 and the last block 20")
}

func TestSharedKeysSkipFreshKeys(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	now := time.Now()
	p.shared.now = func() time.Time { return now }
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}

	route(t, p, pods, e1, tokens(20))
	route(t, p, pods, e1, tokens(20))
	adds, _, _ := store.counts()
	require.Equal(t, 1, adds, "an unchanged prefix is not written again")

	route(t, p, pods, e1, tokens(24))
	require.Equal(t, [][]string{sharedIDs(t, p, tokens(24), 24)}, store.addIDs[1:], "only the new grid key is written")

	now = now.Add(p.shared.ttl/3 + time.Second)
	route(t, p, pods, e1, tokens(20))
	adds, _, _ = store.counts()
	require.Equal(t, 3, adds, "keys are refreshed once a third of the expiry has passed")
}

// sharedIDs returns the shared-key ids of a prompt's blocks at the given
// block counts.
func sharedIDs(t *testing.T, p *dataProducer, tokenIDs []uint32, counts ...int) []string {
	t.Helper()
	req := &fwksched.InferenceRequest{RequestID: "ids", TargetModel: "m", Body: tokenizedBody(tokenIDs)}
	hashes := promptHashes(p, req)
	ids := make([]string, len(counts))
	for i, c := range counts {
		ids[i] = sharedID(hashes[c-1])
	}
	return ids
}

func promptHashes(p *dataProducer, req *fwksched.InferenceRequest) []blockHash {
	require := func(ok bool) {
		if !ok {
			panic("no prompt hashes")
		}
	}
	_ = p.Produce(context.Background(), req, nil)
	state, err := plugin.ReadPluginStateKey[*SchedulingContextState](p.pluginState, req.RequestID, plugin.StateKey(p.typedName.Name))
	require(err == nil && len(state.PerPromptHashes) == 1)
	p.pluginState.Delete(req.RequestID)
	return state.PerPromptHashes[0]
}

func TestSharedKeysRestartedEPPFindsPrefix(t *testing.T) {
	store := newFakeSetStore()
	e1, e2 := testEndpoint("e1"), testEndpoint("e2")
	pods := []fwksched.Endpoint{e1, e2}
	before := newSharingProducer(t, store)
	route(t, before, pods, e2, tokens(40))

	restarted := newSharingProducer(t, store)

	require.Equal(t, []int{0, 40}, matched(t, restarted, pods, tokens(60)), "the engine that holds the prefix scores after a restart")
	_, loads, _ := store.counts()
	require.Equal(t, []int{0, 40}, matched(t, restarted, pods, tokens(40)))
	_, loadsAfter, _ := store.counts()
	require.Equal(t, loads, loadsAfter, "a prefix the local index covers needs no read")
}

func TestSharedKeysMatchOnTheGrid(t *testing.T) {
	store := newFakeSetStore()
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	a := newSharingProducer(t, store)
	route(t, a, pods, e1, tokens(43))

	b := newSharingProducer(t, store)

	require.Equal(t, []int{40}, matched(t, b, pods, tokens(60)), "a longer prompt matches up to the last grid block")
	require.Equal(t, []int{43}, matched(t, newSharingProducer(t, store), pods, tokens(43)), "the same prompt matches in full")
}

func TestSharedKeysSecondEPPSeesLaterTurn(t *testing.T) {
	store := newFakeSetStore()
	e1, e2 := testEndpoint("e1"), testEndpoint("e2")
	pods := []fwksched.Endpoint{e1, e2}
	a, b := newSharingProducer(t, store), newSharingProducer(t, store)

	route(t, a, pods, e1, tokens(16))
	route(t, b, pods, e1, tokens(32))

	require.Equal(t, []int{32, 0}, matched(t, a, pods, tokens(48)), "a reads the keys past its own run")
}

func TestSharedKeysLongPromptNeedsFewReads(t *testing.T) {
	store := newFakeSetStore()
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	route(t, newSharingProducer(t, store), pods, e1, tokens(2048))

	b := newSharingProducer(t, store)
	require.Equal(t, []int{2048}, matched(t, b, pods, tokens(2048)))

	_, loads, _ := store.counts()
	require.LessOrEqual(t, loads, 3)
	for _, ids := range store.loadIDs {
		require.LessOrEqual(t, len(ids), sharedReadFanout)
	}
}

func TestSharedKeysIgnoreEnginesThatLeft(t *testing.T) {
	store := newFakeSetStore()
	gone, e1 := testEndpoint("gone"), testEndpoint("e1")
	route(t, newSharingProducer(t, store), []fwksched.Endpoint{gone, e1}, gone, tokens(16))

	require.Equal(t, []int{0}, matched(t, newSharingProducer(t, store), []fwksched.Endpoint{e1}, tokens(16)))
}

func TestSharedKeysFailedResponseWithdrawsKeys(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	route(t, p, pods, e1, tokens(8))
	req := route(t, p, pods, e1, tokens(20))

	p.ResponseBody(context.Background(), req, failedResponse(req.RequestID), nil)
	p.wg.Wait()

	require.Equal(t, []int{8}, matched(t, newSharingProducer(t, store), pods, tokens(20)), "only the earlier request's prefix stays shared")
	route(t, p, pods, e1, tokens(20))
	require.Equal(t, []int{20}, matched(t, newSharingProducer(t, store), pods, tokens(20)), "a later success publishes the keys again")
}

func TestSharedKeysFallBackToLocalIndex(t *testing.T) {
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}

	t.Run("error", func(t *testing.T) {
		store := newFakeSetStore()
		p := newSharingProducer(t, store)
		route(t, p, pods, e1, tokens(16))
		store.err = errors.New("redis unavailable")

		require.Equal(t, []int{16}, matched(t, p, pods, tokens(32)), "the local index still scores")
		route(t, p, pods, e1, tokens(32))
		store.err = nil
		route(t, p, pods, e1, tokens(32))
		require.Equal(t, []int{32}, matched(t, newSharingProducer(t, store), pods, tokens(32)), "a failed write is retried on the next request")
	})

	t.Run("stalled", func(t *testing.T) {
		store := newFakeSetStore()
		p := newSharingProducer(t, store)
		route(t, p, pods, e1, tokens(16))
		p.shared.writeTimeout = 50 * time.Millisecond
		store.mu.Lock()
		store.stall = true
		store.mu.Unlock()

		started := time.Now()
		require.Equal(t, []int{16}, matched(t, p, pods, tokens(32)))
		require.Less(t, time.Since(started), time.Second, "scoring returns by the read timeout")
	})
}

func TestSharedKeysOffWithoutConfigOrStore(t *testing.T) {
	store := newFakeSetStore()
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}

	disableMinBlockSizeClamp(t)
	cfg := sharedKeysConfig()
	cfg.SharedPrefixKeys = nil
	h := plugin.NewEppHandle(context.Background(), nil, plugin.WithMetricsRecorder(prometheus.NewRegistry()))
	h.SetCrossReplicaSyncer(store)
	off, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, cfg, h)
	require.NoError(t, err)
	route(t, off, pods, e1, tokens(16))
	adds, loads, _ := store.counts()
	require.Zero(t, adds+loads, "no shared keys without sharedPrefixKeys")

	noStore, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, sharedKeysConfig(), testHandleWithSyncer())
	require.NoError(t, err)
	route(t, noStore, pods, e1, tokens(16))
	require.Equal(t, []int{16}, matched(t, noStore, pods, tokens(16)), "a syncer without a set store leaves the local index in charge")
}

func TestSharedKeysConfigValidation(t *testing.T) {
	for name, shared := range map[string]sharedPrefixKeysConfig{
		"bad ttl":          {TTL: "soon"},
		"short ttl":        {TTL: "1ms"},
		"bad read timeout": {ReadTimeout: "-1s"},
		"negative grid":    {GridBlocks: -1},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := sharedKeysConfig()
			cfg.SharedPrefixKeys = &shared
			_, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, cfg, testHandle())
			require.ErrorContains(t, err, "sharedPrefixKeys")
		})
	}
	p, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, sharedKeysConfig(), testHandle())
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, p.shared.ttl)
	require.Equal(t, 8, p.shared.gridBlocks)
	require.Equal(t, 50*time.Millisecond, p.shared.readTimeout)
}

func TestSharedKeysSearchNarrowsToTheLongestPrefix(t *testing.T) {
	store := newFakeSetStore()
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	route(t, newSharingProducer(t, store), pods, e1, tokens(1000))

	_, before, _ := store.counts()
	require.Equal(t, 1, before, "a prompt no replica has seen costs one read")

	b := newSharingProducer(t, store)
	require.Equal(t, []int{1000}, matched(t, b, pods, tokens(2048)), "the second read finds the last shared grid block")

	_, loads, _ := store.counts()
	require.Equal(t, 2, loads-before)
}

func TestSharedKeysFailedResponseDoesNotWaitForPublish(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	p.shared.writeTimeout = time.Minute
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	store.gate, store.entered = make(chan struct{}), make(chan struct{}, 1)

	req := &fwksched.InferenceRequest{RequestID: "held-write", TargetModel: "m", Body: tokenizedBody(tokens(20))}
	require.NoError(t, p.Produce(context.Background(), req, pods))
	res := &fwksched.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults:     map[string]*fwksched.ProfileRunResult{"default": {TargetEndpoints: []fwksched.Endpoint{e1}}},
	}
	require.NoError(t, p.PreRequest(context.Background(), req, res))
	<-store.entered

	done := make(chan struct{})
	go func() {
		p.ResponseBody(context.Background(), req, failedResponse(req.RequestID), nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(store.gate)
		<-done
		t.Fatal("the failed response waited for the shared-key write")
	}
	// Let the rollback's withdrawal reach the store before the write does.
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.removes == 1
	}, 5*time.Second, time.Millisecond)
	close(store.gate)
	p.wg.Wait()

	_, _, keys := store.counts()
	require.Zero(t, keys, "a write that lands after the rollback is withdrawn")
	require.Equal(t, []int{0}, matched(t, p, pods, tokens(20)))
}

// searchRange returns a producer, n chained hashes and their positions 1..n,
// standing in for a prompt with a shared key at every block.
func searchRange(t *testing.T, store *fakeSetStore, n int) (*dataProducer, []blockHash, []int) {
	t.Helper()
	p := newSharingProducer(t, store)
	hashes, positions := make([]blockHash, n), make([]int, n)
	for i := range hashes {
		hashes[i], positions[i] = blockHash(i+1), i+1
	}
	return p, hashes, positions
}

func TestSearchSharedPrefixEdges(t *testing.T) {
	const member = "default/e1"
	podByID := map[string]int{member: 0}
	for _, tc := range []struct {
		positions int
		// shortBy is how far below the longest stored prefix the result may be.
		shortBy int
	}{
		{positions: 1},
		{positions: 2},
		{positions: sharedReadFanout},
		{positions: sharedReadFanout + 1},
		{positions: 257},
		{positions: 3856},
		{positions: 4096, shortBy: 1},
	} {
		store := newFakeSetStore()
		p, hashes, positions := searchRange(t, store, tc.positions)
		// held grows by one each step, so every boundary is searched.
		for held := 0; held <= tc.positions; held++ {
			if held > 0 {
				require.NoError(t, store.AddToSets(t.Context(), p.shared.stateKey, []string{sharedID(hashes[held-1])}, member, time.Minute))
			}
			store.loadIDs = nil
			found, err := p.searchSharedPrefix(t.Context(), store, hashes, positions, podByID)
			require.NoError(t, err)
			if found[0] > held || found[0] < held-tc.shortBy {
				t.Fatalf("%d positions, %d held: found %d", tc.positions, held, found[0])
			}
			if held == 0 && len(store.loadIDs) != 1 {
				t.Fatalf("%d positions, none held: %d reads", tc.positions, len(store.loadIDs))
			}
			if len(store.loadIDs) > sharedReadRounds {
				t.Fatalf("%d positions, %d held: %d reads", tc.positions, held, len(store.loadIDs))
			}
			for _, ids := range store.loadIDs {
				require.LessOrEqual(t, len(ids), sharedReadFanout)
			}
		}
	}
}

func TestSearchSharedPrefixWithExpiredMiddleKeys(t *testing.T) {
	const member = "default/e1"
	store := newFakeSetStore()
	p, hashes, positions := searchRange(t, store, 100)
	var ids []string
	for c := 1; c <= 100; c++ {
		// Keys 11 to 29 and 41 onward are not stored.
		if c <= 10 || (c >= 30 && c <= 40) {
			ids = append(ids, sharedID(hashes[c-1]))
		}
	}
	require.NoError(t, store.AddToSets(t.Context(), p.shared.stateKey, ids, member, time.Minute))
	store.loadIDs = nil

	found, err := p.searchSharedPrefix(t.Context(), store, hashes, positions, map[string]int{member: 0})
	require.NoError(t, err)
	require.Equal(t, 40, found[0], "a later stored key counts although shorter ones expired")
	require.LessOrEqual(t, len(store.loadIDs), sharedReadRounds)
}

func TestSpreadIndexes(t *testing.T) {
	for _, tc := range []struct{ lo, hi int }{{-1, 0}, {-1, 1}, {-1, 16}, {-1, 17}, {-1, 18}, {4, 21}, {4, 22}, {-1, 4096}, {100, 100000}} {
		got := spreadIndexes(tc.lo, tc.hi, sharedReadFanout)
		size := tc.hi - tc.lo - 1
		require.Len(t, got, min(size, sharedReadFanout), "range (%d, %d)", tc.lo, tc.hi)
		for k, idx := range got {
			require.Greater(t, idx, tc.lo)
			require.Less(t, idx, tc.hi)
			if k > 0 {
				require.Greater(t, idx, got[k-1], "range (%d, %d): indexes ascend without repeats", tc.lo, tc.hi)
			}
		}
		if size > 0 {
			require.Equal(t, tc.lo+1, got[0])
			require.Equal(t, tc.hi-1, got[len(got)-1])
		}
	}
}

func TestSharedKeysFollowUpTurnReadsOnlyKeysPastTheLocalRun(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	route(t, p, pods, e1, tokens(20))
	store.loadIDs = nil

	require.Equal(t, []int{20}, matched(t, p, pods, tokens(20)))
	require.Empty(t, store.loadIDs, "a prompt the local index covers costs no read")

	require.Equal(t, []int{20}, matched(t, p, pods, tokens(45)))
	require.Equal(t, [][]string{sharedIDs(t, p, tokens(45), 24, 32, 40, 45)}, store.loadIDs,
		"one read of the grid keys past block 20")
}

func TestSharedKeysFreshRecordsStayCapped(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	now := time.Now()
	p.shared.now = func() time.Time { return now }
	e1 := testEndpoint("e1")
	pods := []fwksched.Endpoint{e1}
	fill := func(written time.Time) {
		for i := range sharedFreshCap {
			p.shared.fresh[sharedMark{endpointID: "filler", hash: blockHash(i)}] = written
		}
	}

	fill(now.Add(-p.shared.ttl))
	route(t, p, pods, e1, tokens(20))
	require.Len(t, p.shared.fresh, 3, "records too old to hold back a write are dropped")

	fill(now)
	route(t, p, pods, e1, tokens(24))
	require.Len(t, p.shared.fresh, 1, "records that are all current are cleared at the cap")

	adds, _, _ := store.counts()
	route(t, p, pods, e1, tokens(20))
	addsAfter, _, _ := store.counts()
	require.Equal(t, adds+1, addsAfter, "a cleared record costs one more write")
	require.Equal(t, []int{24}, matched(t, newSharingProducer(t, store), pods, tokens(24)), "the shared keys are unchanged")
}

func TestSharedKeysRollbackPairsPromptsAndServers(t *testing.T) {
	store := newFakeSetStore()
	p := newSharingProducer(t, store)
	decode, prefill := testEndpoint("decode"), testEndpoint("prefill")
	pods := []fwksched.Endpoint{decode, prefill}
	first, second := tokens(16), []uint32{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112}
	// An earlier request leaves the first prompt on the decode endpoint.
	route(t, p, pods, decode, first)

	req := &fwksched.InferenceRequest{RequestID: "two-prompts", TargetModel: "m", Body: &fwkrequest.InferenceRequestBody{
		TokenizedRequest: &fwkrequest.TokenizedRequest{Prompts: []fwkrequest.PromptTokens{{TokenIDs: first}, {TokenIDs: second}}},
	}}
	require.NoError(t, p.Produce(context.Background(), req, pods))
	res := &fwksched.SchedulingResult{
		PrimaryProfileName: "decode",
		ProfileResults: map[string]*fwksched.ProfileRunResult{
			"decode":                          {TargetEndpoints: []fwksched.Endpoint{decode}},
			experimentalDefaultPrefillProfile: {TargetEndpoints: []fwksched.Endpoint{prefill}},
		},
	}
	require.NoError(t, p.PreRequest(context.Background(), req, res))
	p.wg.Wait()
	fresh := newSharingProducer(t, store)
	require.Equal(t, []int{16, 16}, matched(t, fresh, pods, first))
	require.Equal(t, []int{12, 12}, matched(t, fresh, pods, second))

	p.ResponseBody(context.Background(), req, failedResponse(req.RequestID), nil)
	p.wg.Wait()

	fresh = newSharingProducer(t, store)
	require.Equal(t, []int{16, 0}, matched(t, fresh, pods, first), "the earlier request's keys keep the decode endpoint")
	require.Equal(t, []int{0, 0}, matched(t, fresh, pods, second), "the failed request's own keys lose both endpoints")
}
