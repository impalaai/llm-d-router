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
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Endpoint labels the llmd-manager writes into inventory.list and the resolver
// renders into /shared/endpoints.yaml (spec §4.1). file-discovery copies them
// verbatim into EndpointMetadata.Labels (discovery/file/plugin.go:278 @v0.10.0).
const (
	LabelState     = "impala.ai/state"
	LabelPhase     = "impala.ai/phase"
	LabelShare     = "impala.ai/share"
	LabelBudgetTPM = "impala.ai/budget-tpm"
	LabelBox       = "impala.ai/box"

	StateActive   = "active" // the label is ABSENT for an active endpoint; never written
	StateWarming  = "warming"
	StateDraining = "draining"

	PhaseRetain  = "retain"
	PhaseMigrate = "migrate"
)

// lifecycle is one endpoint's parsed impala.ai/* labels.
type lifecycle struct {
	State     string
	Phase     string  // draining only
	Share     float64 // warming only, in [0, 1]
	BudgetTPM int64   // migrate only, tokens per minute for THIS EPP replica
	Box       string  // optional, any state
}

// parseLifecycle reads an endpoint's labels. Any malformed or unknown value
// makes the endpoint ACTIVE (contract: "Malformed/unknown values -> treat
// endpoint as active and increment the label-error counter"); the returned
// error says which label, and the caller counts it. Labels a state does not
// use are ignored, not validated: a stale impala.ai/phase on an active row
// is not an error.
func parseLifecycle(labels map[string]string) (lifecycle, error) {
	lc := lifecycle{State: StateActive, Box: labels[LabelBox]}
	state, ok := labels[LabelState]
	if !ok {
		return lc, nil
	}
	switch state {
	case StateWarming:
		raw, ok := labels[LabelShare]
		if !ok {
			return lc, fmt.Errorf("%s=warming requires %s", LabelState, LabelShare)
		}
		// strconv.ParseFloat is strict: "0.25 " (trailing space) and "" fail,
		// "1" and "1.0" both parse to 1.
		share, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(share) || math.IsInf(share, 0) || share < 0 || share > 1 {
			return lc, fmt.Errorf("%s=%q is not a decimal in [0,1]", LabelShare, raw)
		}
		lc.State, lc.Share = StateWarming, share
		return lc, nil
	case StateDraining:
		phase, ok := labels[LabelPhase]
		if !ok {
			return lc, fmt.Errorf("%s=draining requires %s", LabelState, LabelPhase)
		}
		switch phase {
		case PhaseRetain:
			lc.State, lc.Phase = StateDraining, PhaseRetain
			return lc, nil
		case PhaseMigrate:
			raw, ok := labels[LabelBudgetTPM]
			if !ok {
				return lc, fmt.Errorf("%s=migrate requires %s", LabelPhase, LabelBudgetTPM)
			}
			budget, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || budget < 1 {
				return lc, fmt.Errorf("%s=%q is not a positive integer", LabelBudgetTPM, raw)
			}
			lc.State, lc.Phase, lc.BudgetTPM = StateDraining, PhaseMigrate, budget
			return lc, nil
		default:
			return lc, fmt.Errorf("%s=%q is not retain or migrate", LabelPhase, phase)
		}
	default:
		return lc, errors.New(LabelState + "=" + strconv.Quote(state) + " is not warming or draining")
	}
}
