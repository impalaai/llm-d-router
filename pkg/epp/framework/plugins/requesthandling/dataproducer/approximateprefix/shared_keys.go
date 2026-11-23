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
	"fmt"
	"strconv"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	defaultSharedPrefixTTL         = 10 * time.Minute
	defaultSharedPrefixGridBlocks  = 8
	defaultSharedPrefixReadTimeout = 50 * time.Millisecond
	sharedPrefixWriteTimeout       = time.Second
	// minSharedPrefixTTL keeps the refresh interval, a third of the
	// expiration, well above one request's duration.
	minSharedPrefixTTL = time.Second

	// sharedReadFanout is the number of shared keys one read asks for.
	sharedReadFanout = 16
	// sharedReadRounds bounds the reads one prompt can cost. Three rounds
	// cover 4096 grid positions.
	sharedReadRounds = 3
	// sharedFreshCap bounds the record of keys this replica wrote.
	sharedFreshCap = 200_000
)

// sharedPrefixKeysConfig turns on shared prefix keys when present, even when
// empty. The configured CrossReplicaSyncer must implement
// datalayer.CrossReplicaSetStore.
type sharedPrefixKeysConfig struct {
	// TTL is how long a shared key lives after its last write. Defaults to 10m.
	TTL string `json:"ttl,omitempty"`
	// GridBlocks is the spacing, in blocks, of the prefix lengths that get a
	// shared key. Defaults to 8.
	GridBlocks int `json:"gridBlocks,omitempty"`
	// ReadTimeout bounds the shared-key reads of one request. Defaults to 50ms.
	ReadTimeout string `json:"readTimeout,omitempty"`
}

// sharedPrefixKeys publishes which endpoints hold a prompt prefix as one set
// per prefix in a CrossReplicaSetStore, and reads those sets when the local
// index does not cover a prompt. A replica that starts with an empty index
// recovers the prefixes other replicas, or its predecessor, published.
type sharedPrefixKeys struct {
	stateKey     datalayer.StateKey
	ttl          time.Duration
	gridBlocks   int
	readTimeout  time.Duration
	writeTimeout time.Duration
	now          func() time.Time
	warnNoStore  sync.Once

	mu sync.Mutex
	// fresh holds when this replica last wrote each key, so an unchanged
	// prefix is rewritten only once a third of its expiration has passed.
	fresh map[sharedMark]time.Time
}

type sharedMark struct {
	endpointID string
	hash       blockHash
}

func newSharedPrefixKeys(name string, cfg *sharedPrefixKeysConfig) (*sharedPrefixKeys, error) {
	if cfg == nil {
		return nil, nil //nolint:nilnil // nil means the feature is off
	}
	s := &sharedPrefixKeys{
		stateKey:     datalayer.StateKey("approx-prefix-shared:" + name),
		ttl:          defaultSharedPrefixTTL,
		gridBlocks:   defaultSharedPrefixGridBlocks,
		readTimeout:  defaultSharedPrefixReadTimeout,
		writeTimeout: sharedPrefixWriteTimeout,
		now:          time.Now,
		fresh:        make(map[sharedMark]time.Time),
	}
	if cfg.TTL != "" {
		ttl, err := time.ParseDuration(cfg.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid configuration: sharedPrefixKeys.ttl %q: %w", cfg.TTL, err)
		}
		if ttl < minSharedPrefixTTL {
			return nil, fmt.Errorf("invalid configuration: sharedPrefixKeys.ttl must be at least %s (current value: %s)", minSharedPrefixTTL, ttl)
		}
		s.ttl = ttl
	}
	if cfg.ReadTimeout != "" {
		timeout, err := time.ParseDuration(cfg.ReadTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid configuration: sharedPrefixKeys.readTimeout %q: %w", cfg.ReadTimeout, err)
		}
		if timeout <= 0 {
			return nil, fmt.Errorf("invalid configuration: sharedPrefixKeys.readTimeout must be positive (current value: %s)", timeout)
		}
		s.readTimeout = timeout
	}
	if cfg.GridBlocks < 0 {
		return nil, fmt.Errorf("invalid configuration: sharedPrefixKeys.gridBlocks must be >= 0 (current value: %d)", cfg.GridBlocks)
	}
	if cfg.GridBlocks > 0 {
		s.gridBlocks = cfg.GridBlocks
	}
	return s, nil
}

// gridPositions returns the prefix lengths, in blocks, that have a shared key
// for a prompt of n blocks and are longer than known: every multiple of grid
// and the whole prompt.
func gridPositions(n, grid, known int) []int {
	var positions []int
	for c := (known/grid + 1) * grid; c <= n; c += grid {
		positions = append(positions, c)
	}
	if n > known && n%grid != 0 {
		positions = append(positions, n)
	}
	return positions
}

func sharedID(hash blockHash) string {
	return strconv.FormatUint(uint64(hash), 36)
}

// sharedStore returns the set store to share prefix keys through, or nil when
// shared keys are off or the configured syncer cannot store sets.
func (p *dataProducer) sharedStore(ctx context.Context) datalayer.CrossReplicaSetStore {
	if p.shared == nil || p.handle == nil {
		return nil
	}
	syncer := p.handle.CrossReplicaSyncer()
	if syncer == nil {
		return nil
	}
	store, ok := syncer.(datalayer.CrossReplicaSetStore)
	if !ok {
		p.shared.warnNoStore.Do(func() {
			log.FromContext(ctx).Info("sharedPrefixKeys is set but the cross-replica syncer cannot store sets; prefix keys are not shared",
				"syncer", syncer.TypedName().String())
		})
		return nil
	}
	return store
}

// publishSharedPrefix writes the grid keys of a prompt routed to endpointID,
// leaving out keys this replica wrote within a third of the expiration. A
// failed write is not recorded, so the next request for the prefix retries it.
func (p *dataProducer) publishSharedPrefix(endpointID string, hashes []blockHash) {
	ctx := p.handle.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	store := p.sharedStore(ctx)
	if store == nil {
		return
	}
	s := p.shared
	now := s.now()
	var ids []string
	var marks []sharedMark
	s.mu.Lock()
	for _, c := range gridPositions(len(hashes), s.gridBlocks, 0) {
		mark := sharedMark{endpointID: endpointID, hash: hashes[c-1]}
		if written, ok := s.fresh[mark]; ok && now.Sub(written) < s.ttl/3 {
			continue
		}
		ids = append(ids, sharedID(mark.hash))
		marks = append(marks, mark)
	}
	s.mu.Unlock()
	if len(ids) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()
	if err := store.AddToSets(ctx, s.stateKey, ids, endpointID, s.ttl); err != nil {
		log.FromContext(ctx).V(logutil.DEBUG).Info("Failed to publish shared prefix keys", "endpoint", endpointID, "keys", len(ids), "error", err.Error())
		return
	}
	s.mu.Lock()
	if len(s.fresh)+len(marks) > sharedFreshCap {
		s.pruneFreshLocked(now)
	}
	for _, mark := range marks {
		s.fresh[mark] = now
	}
	s.mu.Unlock()
}

// pruneFreshLocked drops records that no longer suppress a write, and all of
// them when that is not enough.
func (s *sharedPrefixKeys) pruneFreshLocked(now time.Time) {
	for mark, written := range s.fresh {
		if now.Sub(written) >= s.ttl/3 {
			delete(s.fresh, mark)
		}
	}
	if len(s.fresh) > sharedFreshCap/2 {
		clear(s.fresh)
	}
}

// withdrawSharedPrefix removes endpointID from the grid keys of a prompt whose
// blocks are in inserted, the blocks a failed request newly added.
func (p *dataProducer) withdrawSharedPrefix(endpointID string, hashes, inserted []blockHash) {
	ctx := p.handle.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	store := p.sharedStore(ctx)
	if store == nil || len(inserted) == 0 {
		return
	}
	s := p.shared
	added := make(map[blockHash]struct{}, len(inserted))
	for _, hash := range inserted {
		added[hash] = struct{}{}
	}
	var ids []string
	s.mu.Lock()
	for _, c := range gridPositions(len(hashes), s.gridBlocks, 0) {
		hash := hashes[c-1]
		if _, ok := added[hash]; !ok {
			continue
		}
		delete(s.fresh, sharedMark{endpointID: endpointID, hash: hash})
		ids = append(ids, sharedID(hash))
	}
	s.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	p.wg.Go(func() {
		ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
		defer cancel()
		if err := store.RemoveFromSets(ctx, s.stateKey, ids, endpointID); err != nil {
			log.FromContext(ctx).V(logutil.DEBUG).Info("Failed to withdraw shared prefix keys", "endpoint", endpointID, "keys", len(ids), "error", err.Error())
		}
	})
}

// restoreSharedPrefix reads the shared keys of a prompt past the longest run
// the local index holds (matches, per pod) and adds what other replicas
// published to the local index. It reports whether the index changed. Errors
// and timeouts leave the local index as the only source.
func (p *dataProducer) restoreSharedPrefix(ctx context.Context, hashes []blockHash, pods []fwksched.Endpoint, matches []int) bool {
	if len(hashes) == 0 || len(pods) == 0 {
		return false
	}
	store := p.sharedStore(ctx)
	if store == nil {
		return false
	}
	known := 0
	for _, m := range matches {
		known = max(known, m)
	}
	positions := gridPositions(len(hashes), p.shared.gridBlocks, known)
	if len(positions) == 0 {
		return false
	}
	podByID := make(map[string]int, len(pods))
	for j, pod := range pods {
		podByID[pod.GetMetadata().ID.String()] = j
	}

	found, err := p.searchSharedPrefix(ctx, store, hashes, positions, podByID)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.FromContext(ctx).V(logutil.DEBUG).Info("Failed to read shared prefix keys", "error", err.Error())
	}
	changed := false
	for j, count := range found {
		if count > matches[j] {
			p.indexerInst.Add(hashes[:count], p.makeserver(pods[j]))
			changed = true
		}
	}
	return changed
}

// searchSharedPrefix returns, per pod index, the longest position at which a
// read found the pod. A pod that holds a prefix holds every shorter one, so
// each round reads sharedReadFanout positions spread over the range that can
// still hold the longest shared prefix, and the next round narrows to the gap
// above the last position any candidate held.
func (p *dataProducer) searchSharedPrefix(ctx context.Context, store datalayer.CrossReplicaSetStore, hashes []blockHash, positions []int, podByID map[string]int) (map[int]int, error) {
	found := make(map[int]int)
	lo, hi := -1, len(positions)
	for round := 0; round < sharedReadRounds && hi-lo > 1; round++ {
		probe := spreadIndexes(lo, hi, sharedReadFanout)
		ids := make([]string, len(probe))
		for k, idx := range probe {
			ids[k] = sharedID(hashes[positions[idx]-1])
		}
		sets, err := store.LoadSets(ctx, p.shared.stateKey, ids)
		if err != nil {
			return found, err
		}
		newLo, newHi := lo, hi
		for k, idx := range probe {
			hit := false
			for _, member := range sets[k] {
				if j, ok := podByID[member]; ok {
					hit = true
					found[j] = max(found[j], positions[idx])
				}
			}
			if hit {
				newLo, newHi = idx, hi
			} else if newHi == hi {
				newHi = idx
			}
		}
		lo, hi = newLo, newHi
	}
	return found, nil
}

// spreadIndexes returns up to n indexes of the open range (lo, hi): the
// first, the last, and the rest spread evenly between them. The first index
// lets a prompt no replica has seen end the search after one read.
func spreadIndexes(lo, hi, n int) []int {
	size := hi - lo - 1
	if size <= n {
		out := make([]int, 0, size)
		for idx := lo + 1; idx < hi; idx++ {
			out = append(out, idx)
		}
		return out
	}
	out := make([]int, 0, n)
	out = append(out, lo+1)
	// n-1 further indexes end at hi-1 and are step apart, rounding down.
	for k := 1; k < n; k++ {
		out = append(out, lo+1+k*(size-1)/(n-1))
	}
	return out
}
