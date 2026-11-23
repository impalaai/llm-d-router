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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

const setStateKey = fwkdl.StateKey("prefix")

func TestAddToSetsStoresMemberWithExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)

	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a", "b"}, "e1", valueTTL))
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"b"}, "e2", valueTTL))

	members, err := server.Members("llmd:test:set:prefix:a")
	require.NoError(t, err)
	require.Equal(t, []string{"e1"}, members)
	members, err = server.Members("llmd:test:set:prefix:b")
	require.NoError(t, err)
	require.Equal(t, []string{"e1", "e2"}, members)
	require.Equal(t, valueTTL, server.TTL("llmd:test:set:prefix:a"))
	require.Equal(t, valueTTL, server.TTL("llmd:test:set:prefix:b"))
}

func TestAddToSetsResetsExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e1", valueTTL))
	server.SetTTL("llmd:test:set:prefix:a", time.Second)

	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e1", valueTTL))

	require.Equal(t, valueTTL, server.TTL("llmd:test:set:prefix:a"))
}

func TestLoadSetsReturnsMembersInIDOrder(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a", "c"}, "e1", valueTTL))
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"c"}, "e2", valueTTL))

	sets, err := syncer.LoadSets(t.Context(), setStateKey, []string{"a", "missing", "c"})

	require.NoError(t, err)
	require.Len(t, sets, 3)
	require.Equal(t, []string{"e1"}, sets[0])
	require.Empty(t, sets[1])
	require.ElementsMatch(t, []string{"e1", "e2"}, sets[2])
}

func TestLoadSetsDoesNotResetExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e1", valueTTL))
	server.SetTTL("llmd:test:set:prefix:a", time.Second)

	_, err := syncer.LoadSets(t.Context(), setStateKey, []string{"a"})

	require.NoError(t, err)
	require.Equal(t, time.Second, server.TTL("llmd:test:set:prefix:a"))
}

func TestRemoveFromSetsRemovesOnlyTheMember(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newValueStore(t, server)
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a", "b"}, "e1", valueTTL))
	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e2", valueTTL))

	require.NoError(t, syncer.RemoveFromSets(t.Context(), setStateKey, []string{"a", "b", "missing"}, "e1"))

	sets, err := syncer.LoadSets(t.Context(), setStateKey, []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, []string{"e2"}, sets[0])
	require.Empty(t, sets[1])
}

func TestSetStoreSendsNothingForNoIDs(t *testing.T) {
	recorder := &commandRecorder{}
	client := goredis.NewClient(&goredis.Options{})
	client.AddHook(recorder)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	syncer := &RedisSyncer{client: client}

	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, nil, "e1", valueTTL))
	require.NoError(t, syncer.RemoveFromSets(t.Context(), setStateKey, nil, "e1"))
	sets, err := syncer.LoadSets(t.Context(), setStateKey, nil)
	require.NoError(t, err)
	require.Empty(t, sets)
	require.ErrorContains(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e1", 0), "ttl must be at least 1ms")
	require.Empty(t, recorder.commands, "no command may reach Redis")
}

func TestSetStorePropagatesRedisErrors(t *testing.T) {
	recorder := &commandRecorder{err: errors.New("redis unavailable")}
	client := goredis.NewClient(&goredis.Options{})
	client.AddHook(recorder)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	syncer := &RedisSyncer{client: client}

	require.ErrorContains(t, syncer.AddToSets(t.Context(), setStateKey, []string{"a"}, "e1", valueTTL), "redis-syncer: addtosets: redis unavailable")
	require.ErrorContains(t, syncer.RemoveFromSets(t.Context(), setStateKey, []string{"a"}, "e1"), "redis-syncer: removefromsets: redis unavailable")
	_, err := syncer.LoadSets(t.Context(), setStateKey, []string{"a"})
	require.ErrorContains(t, err, "redis-syncer: loadsets: redis unavailable")
}

func TestSetStoreInClusterMode(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "cluster": true, "keyPrefix": "llmd:test"}`, server.Addr()))
	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("id-%d", i)
	}

	require.NoError(t, syncer.AddToSets(t.Context(), setStateKey, ids, "e1", valueTTL))
	sets, err := syncer.LoadSets(t.Context(), setStateKey, ids)
	require.NoError(t, err)
	for i, set := range sets {
		require.Equal(t, []string{"e1"}, set, "id %s", ids[i])
	}
	require.NoError(t, syncer.RemoveFromSets(t.Context(), setStateKey, ids, "e1"))
	require.Empty(t, server.Keys())
}
