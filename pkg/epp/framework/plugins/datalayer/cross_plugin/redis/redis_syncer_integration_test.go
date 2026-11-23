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
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// integrationParamsEnv holds the plugin's JSON parameters for
// TestIntegrationClusterTLS.
const integrationParamsEnv = "REDIS_SYNCER_IT_PARAMS"

// TestIntegrationClusterTLS runs the value store against a real Redis or Redis
// Cluster, such as a managed service with TLS. It skips unless
// REDIS_SYNCER_IT_PARAMS holds the plugin's JSON parameters, for example:
//
//	REDIS_SYNCER_IT_PARAMS='{"address":"127.0.0.1:7001","cluster":true,"tls":{"caFile":"/tmp/ca.pem","serverName":"redis.test"},"passwordFile":"/tmp/redis-password"}' \
//	  go test ./pkg/epp/framework/plugins/datalayer/cross_plugin/redis/ -run TestIntegrationClusterTLS -count=1 -v
//
// The test overrides keyPrefix with a unique it-<uuid> prefix and deletes its
// keys when it ends. It also needs SCAN, which it uses to find its keys.
func TestIntegrationClusterTLS(t *testing.T) {
	raw := os.Getenv(integrationParamsEnv)
	if raw == "" {
		t.Skipf("set %s to run against a real Redis", integrationParamsEnv)
	}
	params := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &params))
	prefix := "it-" + uuid.NewString()
	params["keyPrefix"] = prefix
	encoded, err := json.Marshal(params)
	require.NoError(t, err)
	syncer := newSyncerFromParams(t, string(encoded))
	t.Cleanup(func() { deleteIntegrationKeys(t, syncer.client, prefix+":*") })

	require.NoError(t, syncer.client.Ping(t.Context()).Err())

	t.Run("keys spread across shards", func(t *testing.T) {
		cluster, ok := syncer.client.(*goredis.ClusterClient)
		if !ok {
			t.Skip("not a cluster client")
		}
		for i := range 64 {
			_, _, err := syncer.LoadOrStore(t.Context(), "spread", fmt.Sprint(i), "e1", time.Minute)
			require.NoError(t, err)
		}
		shardsWithKeys := 0
		for _, n := range keysPerMaster(t, cluster, prefix+":kv:spread:*") {
			if n > 0 {
				shardsWithKeys++
			}
		}
		require.GreaterOrEqual(t, shardsWithKeys, 2)
	})

	t.Run("refresh keeps a value alive", func(t *testing.T) {
		_, _, err := syncer.LoadOrStore(t.Context(), "pin", "refreshed", "e1", time.Second)
		require.NoError(t, err)
		_, found, err := syncer.LoadAndRefresh(t.Context(), "pin", "refreshed", 10*time.Second)
		require.NoError(t, err)
		require.True(t, found)

		time.Sleep(1500 * time.Millisecond)
		value, found, err := syncer.LoadAndRefresh(t.Context(), "pin", "refreshed", 10*time.Second)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "e1", value)
	})

	t.Run("value expires without refresh", func(t *testing.T) {
		_, _, err := syncer.LoadOrStore(t.Context(), "pin", "expired", "e1", time.Second)
		require.NoError(t, err)

		time.Sleep(1500 * time.Millisecond)
		_, found, err := syncer.LoadAndRefresh(t.Context(), "pin", "expired", 10*time.Second)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("compare and swap", func(t *testing.T) {
		_, _, err := syncer.LoadOrStore(t.Context(), "pin", "swapped", "e1", time.Minute)
		require.NoError(t, err)

		actual, swapped, err := syncer.CompareAndSwap(t.Context(), "pin", "swapped", "stale", "e2", 10*time.Minute)
		require.NoError(t, err)
		require.False(t, swapped)
		require.Equal(t, "e1", actual)

		actual, swapped, err = syncer.CompareAndSwap(t.Context(), "pin", "swapped", "e1", "e2", 10*time.Minute)
		require.NoError(t, err)
		require.True(t, swapped)
		require.Equal(t, "e2", actual)
		ttl, err := syncer.client.PTTL(t.Context(), syncer.valueKey("pin", "swapped")).Result()
		require.NoError(t, err)
		require.Greater(t, ttl, 9*time.Minute, "the swap sets the new expiration")
	})
}

// forEachNode runs fn on every master of a cluster client, or on the client
// itself.
func forEachNode(ctx context.Context, client goredis.UniversalClient, fn func(context.Context, *goredis.Client) error) error {
	if cluster, ok := client.(*goredis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, fn)
	}
	return fn(ctx, client.(*goredis.Client))
}

func keysPerMaster(t *testing.T, cluster *goredis.ClusterClient, pattern string) []int {
	t.Helper()
	var mu sync.Mutex
	var counts []int
	require.NoError(t, forEachNode(t.Context(), cluster, func(ctx context.Context, node *goredis.Client) error {
		n := 0
		iter := node.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			n++
		}
		mu.Lock()
		counts = append(counts, n)
		mu.Unlock()
		return iter.Err()
	}))
	return counts
}

// deleteIntegrationKeys deletes keys one at a time: keys on one cluster node
// can still hash to different slots.
func deleteIntegrationKeys(t *testing.T, client goredis.UniversalClient, pattern string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, forEachNode(ctx, client, func(ctx context.Context, node *goredis.Client) error {
		iter := node.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			if err := node.Unlink(ctx, iter.Val()).Err(); err != nil {
				return err
			}
		}
		return iter.Err()
	}))
}
