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
	err     error
	stall   bool
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
