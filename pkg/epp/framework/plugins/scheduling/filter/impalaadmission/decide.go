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

import "math"

// candidate is one scheduling candidate as decide sees it.
type candidate struct {
	id    string    // EndpointMetadata.ID.String(); matches BindingLookup keys
	name  string    // EndpointMetadata.ID.Name; the metrics `endpoint` label (ruling P3)
	lc    lifecycle // parsed labels (active on error)
	match float64   // matched prefix TOKENS in this replica's approximate index
	load  float64   // warming load measure (recent bindings or in-flight requests)
}

// decision is one §5.3 outcome for one endpoint; the plugin turns it into
// impala_admission_decisions_total and impala_admission_moved_tokens_total.
type decision struct {
	endpoint    string
	state       string
	phase       string
	kind        string
	movedTokens float64
}

const (
	decisionRetained = "retained"
	decisionSteered  = "steered"
	decisionMoved    = "moved"
	decisionDeferred = "deferred"
	decisionForced   = "forced"
)

type decideInput struct {
	cands        []candidate
	boundID      string // "" = no session, unbound, or bound to a non-candidate
	minAdvantage float64
	random       func() float64 // called at most once, only for a placeable request with a warming candidate
	take         func(c candidate, cost float64) bool
}

func newDecision(c candidate, kind string, moved float64) decision {
	return decision{endpoint: c.name, state: c.lc.State, phase: c.lc.Phase, kind: kind, movedTokens: moved}
}

// decide ports sim.admission_filter (scripts/llmd-router/lifecycle-sim/sim.py in
// impala-gateway) minus the rejected warm_exclusive variant. It is pure apart
// from in.random and in.take. It returns indices into in.cands in input order
// and never an empty list when in.cands is non-empty.
func decide(in decideInput) ([]int, []decision) {
	cands := in.cands
	out := make([]int, 0, len(cands))
	for i := range cands {
		out = append(out, i)
	}
	var decisions []decision

	// advantage(i) = match(i) - max(match(j) for j != i), over the ORIGINAL
	// candidates: the tokens that would be re-prefilled if the request moved.
	advantage := func(i int) float64 {
		best, found := 0.0, false
		for j := range cands {
			if j != i && (!found || cands[j].match > best) {
				best, found = cands[j].match, true
			}
		}
		return cands[i].match - best // best is 0 for a sole candidate
	}

	for i := range cands {
		d := cands[i]
		if d.lc.State != StateDraining {
			continue
		}
		adv := advantage(i)
		belongs := adv >= in.minAdvantage
		if in.boundID != "" {
			belongs = in.boundID == d.id
		}
		cost := math.Max(adv, 0)
		switch {
		case !belongs:
			out = remove(out, i)
			decisions = append(decisions, newDecision(d, decisionSteered, 0))
		case d.lc.Phase != PhaseMigrate:
			decisions = append(decisions, newDecision(d, decisionRetained, 0))
		case in.take(d, cost):
			out = remove(out, i)
			decisions = append(decisions, newDecision(d, decisionMoved, cost))
			// An empty box means "no box": nothing is a sibling of it (the sim
			// guards with `box and ...`).
			if d.lc.Box != "" {
				var siblings []int
				for _, j := range out {
					if cands[j].lc.Box == d.lc.Box && cands[j].lc.State != StateDraining {
						siblings = append(siblings, j)
					}
				}
				if len(siblings) > 0 {
					out = siblings
				}
			}
		default:
			decisions = append(decisions, newDecision(d, decisionDeferred, 0))
		}
	}

	if w, ok := forcedWarming(in, out, advantage); ok {
		decisions = append(decisions, newDecision(cands[w], decisionForced, 0))
		return []int{w}, decisions
	}

	if len(out) == 0 {
		for i := range cands {
			out = append(out, i)
		}
	}
	return out, decisions
}

// forcedWarming decides whether a warming endpoint takes this request outright.
// Only an unbound request that no candidate (judged over the ORIGINAL list) holds
// an advantage for is placeable; the one random draw picks a warming endpoint
// with probability share/len(warming) each, and the overload stop keeps it from
// taking a full request above the settled mean load.
func forcedWarming(in decideInput, out []int, advantage func(int) float64) (int, bool) {
	cands := in.cands
	var warming []int
	for _, j := range out {
		if cands[j].lc.State == StateWarming {
			warming = append(warming, j)
		}
	}
	if len(warming) == 0 || in.boundID != "" {
		return 0, false
	}
	for i := range cands {
		if advantage(i) >= in.minAdvantage {
			return 0, false
		}
	}
	settled, sum := 0, 0.0
	for _, j := range out {
		if cands[j].lc.State != StateWarming {
			settled++
			sum += cands[j].load
		}
	}
	mean := 0.0
	if settled > 0 {
		mean = sum / float64(settled)
	}
	u := in.random()
	acc := 0.0
	for _, w := range warming {
		acc += cands[w].lc.Share / float64(len(warming))
		if u < acc {
			// Overload stop: W is skipped once load(W) >= mean + 1, i.e. a full
			// request above the mean (">= mean" would block an idle pool, spec §12).
			if settled == 0 || cands[w].load < mean+1 {
				return w, true
			}
			return 0, false
		}
	}
	return 0, false
}

// remove drops index i from the kept list; a no-op when absent (Python's
// list.remove would raise; see Global Constraints fact 8).
func remove(kept []int, i int) []int {
	for k, v := range kept {
		if v == i {
			return append(kept[:k:k], kept[k+1:]...)
		}
	}
	return kept
}
