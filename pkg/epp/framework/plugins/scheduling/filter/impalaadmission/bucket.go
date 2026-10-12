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

import "time"

// tokenBucket is the spec §5.3 migrate budget for one endpoint on one EPP
// replica — a port of sim.TokenBucket (lifecycle-sim/sim.py:201-219).
// It starts EMPTY: a full bucket at the retain->migrate transition would
// release burstMinutes of budget at once. A FULL bucket may admit one move
// larger than its capacity (going negative) so a huge session can still
// move, at most once per burst window. Not goroutine-safe: the plugin holds
// its mutex around every call.
type tokenBucket struct {
	budgetTPM int64 // the label value this bucket was built from; a change recreates it
	rate      float64
	capacity  float64
	tokens    float64
	last      time.Time
}

func newTokenBucket(budgetTPM int64, burstMinutes float64, now time.Time) *tokenBucket {
	return &tokenBucket{
		budgetTPM: budgetTPM,
		rate:      float64(budgetTPM) / 60.0,
		capacity:  float64(budgetTPM) * burstMinutes,
		tokens:    0,
		last:      now,
	}
}

// take refills for the elapsed time, then admits cost when it fits or when the
// bucket is full and cost exceeds the capacity. Verbatim sim semantics.
func (b *tokenBucket) take(cost float64, now time.Time) bool {
	// Deliberate divergence from the sim: it lets now < t refill negatively; a
	// wall clock stepping backwards must not drain a bucket.
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(b.capacity, b.tokens+b.rate*elapsed)
	}
	b.last = now
	if cost <= b.tokens || (cost > b.capacity && b.tokens >= b.capacity) {
		b.tokens -= cost
		return true
	}
	return false
}

// level is the token count after the last take; negative while in debt.
func (b *tokenBucket) level() float64 { return b.tokens }
