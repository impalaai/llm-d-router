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
	"github.com/stretchr/testify/require"
)

const blk = 256.0 // B in the sim tests: one prefix block in tokens

func c(name string, match float64) candidate {
	return candidate{id: "default/" + name, name: name, lc: lifecycle{State: StateActive}, match: match}
}

func draining(name string, phase string, match float64) candidate {
	x := c(name, match)
	x.lc = lifecycle{State: StateDraining, Phase: phase, BudgetTPM: 1}
	return x
}

func warming(name string, share, match float64) candidate {
	x := c(name, match)
	x.lc = lifecycle{State: StateWarming, Share: share}
	return x
}

func withBox(x candidate, box string) candidate    { x.lc.Box = box; return x }
func withLoad(x candidate, load float64) candidate { x.load = load; return x }

type run struct {
	cands   []candidate
	bound   string
	u       float64
	take    func(candidate, float64) bool
	rngUsed int
}

func (r *run) do(t *testing.T) ([]string, []decision) {
	t.Helper()
	take := r.take
	if take == nil {
		take = func(candidate, float64) bool { return false }
	}
	kept, decs := decide(decideInput{
		cands:        r.cands,
		boundID:      r.bound,
		minAdvantage: blk,
		random:       func() float64 { r.rngUsed++; return r.u },
		take:         take,
	})
	require.NotEmpty(t, kept, "never empty")
	names := make([]string, 0, len(kept))
	for _, i := range kept {
		names = append(names, r.cands[i].name)
	}
	return names, decs
}

func dec(name, state, phase, kind string, moved float64) decision {
	return decision{endpoint: name, state: state, phase: phase, kind: kind, movedTokens: moved}
}

func takeAll(candidate, float64) bool { return true }

// ---------------------------------------------------------------- draining: retain

func TestDecide_RetainKeepsTheEndpointForItsBoundSession(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 0), c("a", 0)}, bound: "default/d"}
	out, decs := r.do(t)
	assert.Equal(t, []string{"d", "a"}, out)
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseRetain, decisionRetained, 0)}, decs)
}

func TestDecide_RetainSteersANewSessionAway(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 4*blk), c("a", 4*blk)}}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a"}, out)
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseRetain, decisionSteered, 0)}, decs)
}

func TestDecide_RetainKeepsAConversationWhoseHistoryOnlyTheDrainingEndpointHolds(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 14*blk), c("a", 4*blk)}}
	out, _ := r.do(t)
	assert.Contains(t, out, "d")
}

func TestDecide_ASharedSystemPromptAloneDoesNotKeepADrainingEndpoint(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 4*blk), c("a", 4*blk)}}
	out, _ := r.do(t)
	assert.NotContains(t, out, "d")
}

func TestDecide_BoundToNonCandidateIsUnbound(t *testing.T) { // review focus 5
	// The caller passes "" when the bound endpoint is absent from the candidates;
	// the request must then be placed like a new session: steered off d.
	r := &run{cands: []candidate{draining("d", PhaseRetain, 0), c("a", 0)}, bound: ""}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a"}, out)
	assert.Equal(t, decisionSteered, decs[0].kind)
}

func TestDecide_BoundElsewhereSteersOffTheDrainingEndpoint(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 14*blk), c("a", 0)}, bound: "default/a"}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a"}, out)
	assert.Equal(t, decisionSteered, decs[0].kind)
}

// --------------------------------------------------------------- draining: migrate

func TestDecide_MigrateMovesOnlyWhatTheBucketCovers(t *testing.T) {
	take := func(_ candidate, cost float64) bool { return cost <= 10*blk }
	r := &run{cands: []candidate{draining("d", PhaseMigrate, 14*blk), c("a", 4*blk)}, take: take}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a"}, out)
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseMigrate, decisionMoved, 10*blk)}, decs)

	r = &run{cands: []candidate{draining("d", PhaseMigrate, 30*blk), c("a", 4*blk)}, take: take}
	out, decs = r.do(t)
	assert.Contains(t, out, "d")
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseMigrate, decisionDeferred, 0)}, decs)
}

func TestDecide_MigrateMovesABoundSessionWithNoAdvantageForFree(t *testing.T) {
	var costs []float64
	take := func(_ candidate, cost float64) bool { costs = append(costs, cost); return cost <= 0 }
	r := &run{cands: []candidate{draining("d", PhaseMigrate, 4*blk), c("a", 4*blk)}, bound: "default/d", take: take}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a"}, out)
	assert.Equal(t, []float64{0}, costs)
	assert.Equal(t, decisionMoved, decs[0].kind)
	assert.Equal(t, 0.0, decs[0].movedTokens)
}

func TestDecide_AZeroCostMoveIsDeferredWhenTheBucketDoesNotFit(t *testing.T) {
	// Controller ruling: take(0) is false for a bucket in debt (tokens < 0), as in the
	// sim, so decide must not assume a zero-cost move always fits.
	t0 := time.Unix(1_700_000_000, 0)
	b := newTokenBucket(60, 1, t0) // rate 1/s, capacity 60
	require.True(t, b.take(500, t0.Add(time.Hour)), "a full bucket admits one oversized move")
	require.Less(t, b.level(), 0.0, "bucket is in debt")
	take := func(_ candidate, cost float64) bool { return b.take(cost, t0.Add(time.Hour)) }
	r := &run{cands: []candidate{draining("d", PhaseMigrate, 4*blk), c("a", 4*blk)}, bound: "default/d", take: take}
	out, decs := r.do(t)
	assert.Equal(t, []string{"d", "a"}, out)
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseMigrate, decisionDeferred, 0)}, decs)
}

func TestDecide_AMovePrefersTheSameBoxSibling(t *testing.T) {
	r := &run{cands: []candidate{
		withBox(draining("d", PhaseMigrate, 14*blk), "b0"),
		withBox(c("s", 4*blk), "b0"),
		withBox(c("a", 4*blk), "b1"),
	}, take: takeAll}
	out, _ := r.do(t)
	assert.Equal(t, []string{"s"}, out)
}

func TestDecide_AnEmptyBoxIsNoBoxSoNothingIsASibling(t *testing.T) {
	// Controller ruling: lifecycle.Box == "" means no box (the sim guards with `box and ...`).
	// Without the guard every unlabelled endpoint would be a "sibling" of d, and the
	// narrowing would drop c, which carries a (different) box.
	r := &run{cands: []candidate{
		draining("d", PhaseMigrate, 14*blk),
		withBox(c("c", 4*blk), "b1"),
		c("e", 4*blk),
	}, take: takeAll}
	out, decs := r.do(t)
	assert.Equal(t, []string{"c", "e"}, out, "the move is not narrowed to a sibling")
	assert.Equal(t, decisionMoved, decs[0].kind)

	// all-empty boxes: same result
	r = &run{cands: []candidate{draining("d", PhaseMigrate, 14*blk), c("a", 4*blk), c("b", 4*blk)}, take: takeAll}
	out, _ = r.do(t)
	assert.Equal(t, []string{"a", "b"}, out)
}

func TestDecide_ADrainingSiblingIsNotPreferred(t *testing.T) {
	r := &run{cands: []candidate{
		withBox(draining("d", PhaseMigrate, 14*blk), "b0"),
		withBox(draining("s", PhaseRetain, 0), "b0"),
		withBox(c("a", 4*blk), "b1"),
	}, take: takeAll}
	out, _ := r.do(t)
	assert.Equal(t, []string{"a"}, out) // s was steered (new session) and is draining: no sibling narrowing
}

func TestDecide_TwoDrainingEndpointsAfterASiblingNarrowingDoNotPanic(t *testing.T) {
	// Python's list.remove would raise here (Global Constraints fact 8); Go must not.
	r := &run{cands: []candidate{
		withBox(draining("d1", PhaseMigrate, 14*blk), "b0"),
		withBox(c("s", 4*blk), "b0"),
		draining("d2", PhaseRetain, 0),
		c("a", 4*blk),
	}, take: takeAll}
	out, _ := r.do(t)
	assert.Equal(t, []string{"s"}, out)
}

// ---------------------------------------------------------------------- warming

func TestDecide_WarmingTakesAPlaceableRequestWithProbabilityShare(t *testing.T) {
	cands := []candidate{warming("w", 0.25, 0), c("a", 4*blk), c("b", 4*blk)}
	out, decs := (&run{cands: cands, u: 0.2}).do(t)
	assert.Equal(t, []string{"w"}, out)
	assert.Equal(t, []decision{dec("w", StateWarming, "", decisionForced, 0)}, decs)
	out, decs = (&run{cands: cands, u: 0.3}).do(t)
	assert.Equal(t, []string{"w", "a", "b"}, out)
	assert.Empty(t, decs)
}

func TestDecide_WarmingNeverTakesAConversationThatHasAHome(t *testing.T) {
	out, _ := (&run{cands: []candidate{warming("w", 1, 0), c("a", 14*blk), c("b", 4*blk)}, u: 0}).do(t)
	assert.Equal(t, []string{"w", "a", "b"}, out)
	r := &run{cands: []candidate{warming("w", 1, 0), c("a", 4*blk), c("b", 4*blk)}, bound: "default/a", u: 0}
	out, _ = r.do(t)
	assert.Equal(t, []string{"w", "a", "b"}, out)
	assert.Equal(t, 0, r.rngUsed, "a bound request never draws")
}

func TestDecide_WarmingOverloadStop(t *testing.T) {
	cands := []candidate{withLoad(warming("w", 1, 0), 5), withLoad(c("a", 0), 4), withLoad(c("b", 0), 4)}
	out, decs := (&run{cands: cands}).do(t)
	assert.Equal(t, []string{"w", "a", "b"}, out)
	assert.Empty(t, decs)
	// one unit above the mean is the boundary: 4.9 is still forced
	cands[0].load = 4.9
	out, _ = (&run{cands: cands}).do(t)
	assert.Equal(t, []string{"w"}, out)
}

func TestDecide_WarmingOnAnIdlePoolIsForced(t *testing.T) {
	// all loads 0: "w < mean + 1" holds (">= mean" would have blocked an idle pool, §12)
	out, _ := (&run{cands: []candidate{warming("w", 1, 0), c("a", 0), c("b", 0)}}).do(t)
	assert.Equal(t, []string{"w"}, out)
}

func TestDecide_AllWarmingPoolForcesWithoutSettledPeers(t *testing.T) {
	out, _ := (&run{cands: []candidate{withLoad(warming("w", 1, 0), 50)}}).do(t)
	assert.Equal(t, []string{"w"}, out)
}

func TestDecide_TwoWarmingEndpointsSplitTheForcedShare(t *testing.T) {
	cands := []candidate{warming("w1", 0.5, 0), warming("w2", 0.5, 0), c("a", 0)}
	// acc after w1 = 0.25, after w2 = 0.5
	out, _ := (&run{cands: cands, u: 0.1}).do(t)
	assert.Equal(t, []string{"w1"}, out)
	out, _ = (&run{cands: cands, u: 0.3}).do(t)
	assert.Equal(t, []string{"w2"}, out)
	out, _ = (&run{cands: cands, u: 0.6}).do(t)
	assert.Equal(t, []string{"w1", "w2", "a"}, out)
}

func TestDecide_WarmingIsNotPlaceableWhenADrainingEndpointHoldsTheHistory(t *testing.T) {
	// Placeability is judged over the ORIGINAL candidates. d holds the history (advantage
	// 10 blocks) and is moved off the list by migrate; judged over the remaining
	// candidates {w, a} (equal matches) the request WOULD be placeable and w forced.
	cands := []candidate{warming("w", 1, 4*blk), draining("d", PhaseMigrate, 14*blk), c("a", 4*blk)}
	out, decs := (&run{cands: cands, u: 0, take: takeAll}).do(t)
	assert.Equal(t, []string{"w", "a"}, out)
	assert.Equal(t, []decision{dec("d", StateDraining, PhaseMigrate, decisionMoved, 10*blk)}, decs)
}

// ----------------------------------------------------------------------- guards

func TestDecide_NeverReturnsAnEmptyList(t *testing.T) {
	r := &run{cands: []candidate{draining("d", PhaseRetain, 0)}}
	out, decs := r.do(t)
	assert.Equal(t, []string{"d"}, out)
	assert.Equal(t, decisionSteered, decs[0].kind)
}

func TestDecide_NoLabelsMeansPassThroughWithoutTouchingTheRNG(t *testing.T) {
	r := &run{cands: []candidate{c("a", 0), c("b", 0)}}
	out, decs := r.do(t)
	assert.Equal(t, []string{"a", "b"}, out)
	assert.Empty(t, decs)
	assert.Equal(t, 0, r.rngUsed)
}
