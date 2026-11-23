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
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

var _ fwkdl.CrossReplicaSetStore = (*RedisSyncer)(nil)

// addToSetScript adds ARGV[1] to the set at KEYS[1] and sets its expiration to
// ARGV[2] milliseconds in one step, so a set is never stored without an
// expiration.
const addToSetScript = `redis.call('SADD', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1`

func (s *RedisSyncer) setKey(key fwkdl.StateKey, id string) string {
	return s.key("set", string(key), id)
}

// AddToSets sends one EVAL per id in a pipeline. EVAL carries the script text
// because a pipelined EVALSHA cannot fall back when a node lacks the script.
func (s *RedisSyncer) AddToSets(ctx context.Context, key fwkdl.StateKey, ids []string, member string, ttl time.Duration) error {
	if err := checkTTL(ttl); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for _, id := range ids {
		pipe.Eval(ctx, addToSetScript, []string{s.setKey(key, id)}, member, ttl.Milliseconds())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis-syncer: addtosets: %w", err)
	}
	return nil
}

func (s *RedisSyncer) RemoveFromSets(ctx context.Context, key fwkdl.StateKey, ids []string, member string) error {
	if len(ids) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	for _, id := range ids {
		pipe.SRem(ctx, s.setKey(key, id), member)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis-syncer: removefromsets: %w", err)
	}
	return nil
}

func (s *RedisSyncer) LoadSets(ctx context.Context, key fwkdl.StateKey, ids []string) ([][]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*goredis.StringSliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.SMembers(ctx, s.setKey(key, id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis-syncer: loadsets: %w", err)
	}
	sets := make([][]string, len(ids))
	for i, cmd := range cmds {
		sets[i] = cmd.Val()
	}
	return sets, nil
}
