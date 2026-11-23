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

package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

const (
	valueStateKey = fwkdl.StateKey("pin")
	valueID       = "abc"
	valueKey      = "llmd:test:kv:pin:abc"
	valueTTL      = 10 * time.Minute
)

func newValueStore(t *testing.T, server *miniredis.Miniredis) *RedisSyncer {
	t.Helper()
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.keyPrefix = "llmd:test"
	return syncer
}

// newFactoryValueStore builds the syncer through the factory, so the client
// honors context deadlines the way it does in production.
func newFactoryValueStore(t *testing.T, server *miniredis.Miniredis) *RedisSyncer {
	t.Helper()
	return newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "keyPrefix": "llmd:test"}`, server.Addr()))
}

func TestLoadAndRefreshMissing(t *testing.T) {
	syncer := newValueStore(t, miniredis.RunT(t))

	value, found, err := syncer.LoadAndRefresh(t.Context(), valueStateKey, valueID, valueTTL)

	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, value)
}

func TestLoadAndRefreshResetsExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, server.Set(valueKey, "e1"))
	server.SetTTL(valueKey, time.Second)

	value, found, err := syncer.LoadAndRefresh(t.Context(), valueStateKey, valueID, valueTTL)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "e1", value)
	require.Equal(t, valueTTL, server.TTL(valueKey))
}

func TestLoadOrStoreReturnsWinner(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)

	actual, loaded, err := syncer.LoadOrStore(t.Context(), valueStateKey, valueID, "e1", valueTTL)
	require.NoError(t, err)
	require.False(t, loaded)
	require.Equal(t, "e1", actual)

	actual, loaded, err = syncer.LoadOrStore(t.Context(), valueStateKey, valueID, "e2", valueTTL)
	require.NoError(t, err)
	require.True(t, loaded)
	require.Equal(t, "e1", actual)

	stored, err := server.Get(valueKey)
	require.NoError(t, err)
	require.Equal(t, "e1", stored)
	require.Equal(t, valueTTL, server.TTL(valueKey))
}

func TestLoadOrStoreConcurrentCallersAgree(t *testing.T) {
	server := miniredis.RunT(t)
	const callers = 16
	type result struct {
		actual string
		loaded bool
		err    error
	}
	results := make([]result, callers)
	var wg sync.WaitGroup
	for i := range callers {
		syncer := newValueStore(t, server)
		wg.Go(func() {
			actual, loaded, err := syncer.LoadOrStore(context.Background(), valueStateKey, valueID, fmt.Sprintf("e%d", i), valueTTL)
			results[i] = result{actual: actual, loaded: loaded, err: err}
		})
	}
	wg.Wait()

	stored, err := server.Get(valueKey)
	require.NoError(t, err)
	stores := 0
	for _, r := range results {
		require.NoError(t, r.err)
		require.Equal(t, stored, r.actual, "every caller must route to the stored value")
		if !r.loaded {
			stores++
		}
	}
	require.Equal(t, 1, stores, "exactly one caller stores its value")
}

func TestCompareAndSwap(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, server.Set(valueKey, "e1"))
	server.SetTTL(valueKey, time.Second)

	actual, swapped, err := syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e2", valueTTL)
	require.NoError(t, err)
	require.True(t, swapped)
	require.Equal(t, "e2", actual)
	require.Equal(t, valueTTL, server.TTL(valueKey))

	server.SetTTL(valueKey, time.Second)
	actual, swapped, err = syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e3", valueTTL)
	require.NoError(t, err)
	require.False(t, swapped, "a stale old value must not replace the current one")
	require.Equal(t, "e2", actual)
	stored, err := server.Get(valueKey)
	require.NoError(t, err)
	require.Equal(t, "e2", stored)
	require.Equal(t, time.Second, server.TTL(valueKey), "a refused swap leaves the expiry alone")

	server.Del(valueKey)
	actual, swapped, err = syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e9", "e4", valueTTL)
	require.NoError(t, err)
	require.True(t, swapped, "a missing key is stored")
	require.Equal(t, "e4", actual)
}

func TestCompareAndSwapAfterScriptFlush(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)

	_, swapped, err := syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "", "e1", valueTTL)
	require.NoError(t, err)
	require.True(t, swapped)
	require.NoError(t, syncer.client.ScriptFlush(t.Context()).Err())

	actual, swapped, err := syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e2", valueTTL)
	require.NoError(t, err)
	require.True(t, swapped)
	require.Equal(t, "e2", actual)
}

func TestValueStoreRejectsShortTTL(t *testing.T) {
	recorder := &commandRecorder{}
	client := goredis.NewClient(&goredis.Options{})
	client.AddHook(recorder)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	syncer := &RedisSyncer{client: client}

	for _, ttl := range []time.Duration{0, 500 * time.Microsecond} {
		_, _, err := syncer.LoadAndRefresh(t.Context(), valueStateKey, valueID, ttl)
		require.ErrorContains(t, err, "ttl must be at least 1ms")
		_, _, err = syncer.LoadOrStore(t.Context(), valueStateKey, valueID, "e1", ttl)
		require.ErrorContains(t, err, "ttl must be at least 1ms")
		_, _, err = syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e2", ttl)
		require.ErrorContains(t, err, "ttl must be at least 1ms")
	}
	require.Empty(t, recorder.commands, "no command may reach Redis")
}

func TestValueStoreKeysUsePrefix(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)

	_, _, err := syncer.LoadOrStore(t.Context(), valueStateKey, valueID, "e1", valueTTL)
	require.NoError(t, err)
	_, _, err = syncer.LoadAndRefresh(t.Context(), valueStateKey, valueID, valueTTL)
	require.NoError(t, err)
	_, _, err = syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e2", valueTTL)
	require.NoError(t, err)

	require.Equal(t, []string{valueKey}, server.Keys())
}

func TestValueStoreHonorsDeadline(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newFactoryValueStore(t, server)
	require.NoError(t, syncer.client.Ping(t.Context()).Err())
	var received atomic.Bool
	server.Server().SetPreHook(func(_ *miniredisserver.Peer, command string, _ ...string) bool {
		if command == "GETEX" {
			received.Store(true)
			return true
		}
		return false
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err := syncer.LoadAndRefresh(ctx, valueStateKey, valueID, valueTTL)

	require.Error(t, err)
	require.True(t, received.Load(), "Redis must receive the command before its deadline")
	require.Less(t, time.Since(started), time.Second, "a stalled Redis must not hold the caller past its deadline")
}

// TestValueStoreHonorsDeadlineBeforeConnecting covers a Redis that accepts
// connections and never answers, met by a client with no pooled connection
// and, in cluster mode, no slot map.
func TestValueStoreHonorsDeadlineBeforeConnecting(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	for _, params := range []string{
		`{"address": %q}`,
		`{"address": %q, "tls": {}}`,
		`{"address": %q, "cluster": true}`,
		`{"address": %q, "cluster": true, "tls": {}}`,
	} {
		t.Run(params, func(t *testing.T) {
			syncer := newSyncerFromParams(t, fmt.Sprintf(params, listener.Addr().String()))
			for name, call := range map[string]func(context.Context) error{
				"LoadAndRefresh": func(ctx context.Context) error {
					_, _, err := syncer.LoadAndRefresh(ctx, valueStateKey, valueID, valueTTL)
					return err
				},
				"LoadOrStore": func(ctx context.Context) error {
					_, _, err := syncer.LoadOrStore(ctx, valueStateKey, valueID, "e1", valueTTL)
					return err
				},
				"CompareAndSwap": func(ctx context.Context) error {
					_, _, err := syncer.CompareAndSwap(ctx, valueStateKey, valueID, "e1", "e2", valueTTL)
					return err
				},
			} {
				ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
				started := time.Now()
				err := call(ctx)
				cancel()
				require.Error(t, err, name)
				require.Less(t, time.Since(started), time.Second, name)
			}
		})
	}
}

func TestValueStorePropagatesRedisErrors(t *testing.T) {
	recorder := &commandRecorder{err: errors.New("redis unavailable")}
	client := goredis.NewClient(&goredis.Options{})
	client.AddHook(recorder)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	syncer := &RedisSyncer{client: client}

	_, _, err := syncer.LoadAndRefresh(t.Context(), valueStateKey, valueID, valueTTL)
	require.ErrorContains(t, err, "redis-syncer: loadandrefresh: redis unavailable")
	_, _, err = syncer.LoadOrStore(t.Context(), valueStateKey, valueID, "e1", valueTTL)
	require.ErrorContains(t, err, "redis-syncer: loadorstore: redis unavailable")
	_, _, err = syncer.CompareAndSwap(t.Context(), valueStateKey, valueID, "e1", "e2", valueTTL)
	require.ErrorContains(t, err, "redis-syncer: compareandswap: redis unavailable")
}
