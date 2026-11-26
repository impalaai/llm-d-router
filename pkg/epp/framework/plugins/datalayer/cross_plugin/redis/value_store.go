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
	"time"

	goredis "github.com/redis/go-redis/v9"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/cross_plugin/internal/crossreplica"
)

var _ fwkdl.CrossReplicaValueStore = (*RedisSyncer)(nil)

// compareAndSwapScript stores ARGV[2] with an expiration of ARGV[3]
// milliseconds when KEYS[1] holds ARGV[1] or nothing. It returns {1, stored
// value} after a swap and {0, current value} otherwise.
var compareAndSwapScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == false or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return {1, ARGV[2]}
end
return {0, current}
`)

// valueKey namespaces value-store keys away from endpoint hashes and
// GetOrSet's gob-encoded coordination values.
func (s *RedisSyncer) valueKey(key fwkdl.StateKey, id string) string {
	return s.key("kv", string(key), id)
}

// checkTTL rejects expirations Redis cannot apply. go-redis turns a zero
// expiration on GETEX into PERSIST, which would keep the key forever.
func checkTTL(ttl time.Duration) error {
	if ttl < crossreplica.MinimumDuration {
		return fmt.Errorf("redis-syncer: ttl must be at least %s, got %s", crossreplica.MinimumDuration, ttl)
	}
	return nil
}

// LoadAndRefresh reads the value with GETEX, which needs Redis 6.2 or newer.
func (s *RedisSyncer) LoadAndRefresh(ctx context.Context, key fwkdl.StateKey, id string, ttl time.Duration) (string, bool, error) {
	if err := checkTTL(ttl); err != nil {
		return "", false, err
	}
	value, err := s.client.GetEx(ctx, s.valueKey(key, id), ttl).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis-syncer: loadandrefresh: %w", err)
	}
	return value, true, nil
}

// LoadOrStore uses SET NX GET, which needs Redis 7.0 or newer.
func (s *RedisSyncer) LoadOrStore(ctx context.Context, key fwkdl.StateKey, id, value string, ttl time.Duration) (string, bool, error) {
	if err := checkTTL(ttl); err != nil {
		return "", false, err
	}
	stored, err := s.client.SetArgs(ctx, s.valueKey(key, id), value, goredis.SetArgs{
		Mode: "NX",
		Get:  true,
		TTL:  ttl,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return value, false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("redis-syncer: loadorstore: %w", err)
	}
	return stored, true, nil
}

// CompareAndSwap runs compareAndSwapScript with EVALSHA, falling back to EVAL
// when the server does not have the script cached.
func (s *RedisSyncer) CompareAndSwap(ctx context.Context, key fwkdl.StateKey, id, old, value string, ttl time.Duration) (string, bool, error) {
	if err := checkTTL(ttl); err != nil {
		return "", false, err
	}
	reply, err := compareAndSwapScript.Run(ctx, s.client, []string{s.valueKey(key, id)}, old, value, ttl.Milliseconds()).Slice()
	if err != nil {
		return "", false, fmt.Errorf("redis-syncer: compareandswap: %w", err)
	}
	if len(reply) != 2 {
		return "", false, fmt.Errorf("redis-syncer: compareandswap: unexpected reply %v", reply)
	}
	swapped, okSwapped := reply[0].(int64)
	actual, okActual := reply[1].(string)
	if !okSwapped || !okActual {
		return "", false, fmt.Errorf("redis-syncer: compareandswap: unexpected reply %v", reply)
	}
	return actual, swapped == 1, nil
}
