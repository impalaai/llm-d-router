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

package impalaadmission

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenBucket_StartsEmptyAndLetsOneOversizedMoveThroughWhenFull(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }

	b := newTokenBucket(60_000, 1, t0) // 1000 tok/s, capacity 60k (sim vector)
	assert.False(t, b.take(1, at(0)), "empty at migrate start")
	assert.True(t, b.take(30_000, at(30)))
	assert.False(t, b.take(100_000, at(30)), "bigger than capacity, bucket not full")
	assert.True(t, b.take(100_000, at(120)), "full: one oversized move")
	assert.False(t, b.take(1, at(120)), "now in debt")
	assert.Less(t, b.level(), 0.0)
}

func TestTokenBucket_ZeroCostAlwaysFits(t *testing.T) {
	// sim: cost <= tokens with cost 0 and tokens 0 is True, so a session bound
	// to the draining engine with no prefix advantage moves for free.
	b := newTokenBucket(120_000, 5, time.Unix(0, 0))
	assert.True(t, b.take(0, time.Unix(0, 0)))
}

func TestTokenBucket_RefillIsCappedAtCapacity(t *testing.T) {
	t0 := time.Unix(0, 0)
	b := newTokenBucket(60_000, 1, t0)
	assert.True(t, b.take(0, t0.Add(time.Hour))) // refill settles at capacity, not 3.6M
	assert.InDelta(t, 60_000, b.level(), 1e-6)
}

func TestTokenBucket_ClockStepBackwardsDoesNotDrain(t *testing.T) {
	t0 := time.Unix(1_000, 0)
	b := newTokenBucket(60_000, 1, t0)
	assert.True(t, b.take(0, t0.Add(30*time.Second))) // 30k refilled
	assert.True(t, b.take(0, t0))                     // clock steps back 30 s
	assert.InDelta(t, 30_000, b.level(), 1e-6)
}
