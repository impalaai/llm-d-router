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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		labels  map[string]string
		want    lifecycle
		wantErr string // "" = no error
	}{
		{name: "no labels is active", labels: nil, want: lifecycle{State: StateActive}},
		{name: "unrelated labels are ignored", labels: map[string]string{"app": "x"}, want: lifecycle{State: StateActive}},
		{
			name: "an explicit state=active is accepted silently", labels: map[string]string{LabelState: "active"},
			want: lifecycle{State: StateActive},
		},
		{
			name:   "an explicit state=active keeps its box and ignores stray labels",
			labels: map[string]string{LabelState: "active", LabelBox: "eu6", LabelShare: "x", LabelPhase: "bogus"},
			want:   lifecycle{State: StateActive, Box: "eu6"},
		},
		{
			name: "box is carried on an active endpoint", labels: map[string]string{LabelBox: "eu6"},
			want: lifecycle{State: StateActive, Box: "eu6"},
		},
		{
			name: "draining retain", labels: map[string]string{LabelState: "draining", LabelPhase: "retain"},
			want: lifecycle{State: StateDraining, Phase: PhaseRetain},
		},
		{
			name: "draining migrate with budget", labels: map[string]string{
				LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "100000", LabelBox: "eu6",
			},
			want: lifecycle{State: StateDraining, Phase: PhaseMigrate, BudgetTPM: 100000, Box: "eu6"},
		},
		{
			name: "warming share 0.25", labels: map[string]string{LabelState: "warming", LabelShare: "0.25"},
			want: lifecycle{State: StateWarming, Share: 0.25},
		},
		{
			name:   "warming share 1 and 1.0 are both full share (review focus 4)",
			labels: map[string]string{LabelState: "warming", LabelShare: "1"},
			want:   lifecycle{State: StateWarming, Share: 1},
		},
		{
			name: "warming share 1.0", labels: map[string]string{LabelState: "warming", LabelShare: "1.0"},
			want: lifecycle{State: StateWarming, Share: 1},
		},
		{
			name:   "warming share with trailing space is malformed (review focus 4)",
			labels: map[string]string{LabelState: "warming", LabelShare: "0.25 "},
			want:   lifecycle{State: StateActive}, wantErr: "impala.ai/share",
		},
		{
			name: "warming share above 1 is malformed", labels: map[string]string{LabelState: "warming", LabelShare: "1.5"},
			want: lifecycle{State: StateActive}, wantErr: "impala.ai/share",
		},
		{
			name:   "warming share 0 and the 4-decimal edges are accepted",
			labels: map[string]string{LabelState: "warming", LabelShare: "0"},
			want:   lifecycle{State: StateWarming, Share: 0},
		},
		{
			name: "warming share 0.0001", labels: map[string]string{LabelState: "warming", LabelShare: "0.0001"},
			want: lifecycle{State: StateWarming, Share: 0.0001},
		},
		{
			name: "warming share 1.0000", labels: map[string]string{LabelState: "warming", LabelShare: "1.0000"},
			want: lifecycle{State: StateWarming, Share: 1},
		},
		{
			name: "warming without share is malformed", labels: map[string]string{LabelState: "warming"},
			want: lifecycle{State: StateActive}, wantErr: "impala.ai/share",
		},
		{
			name: "unknown state is malformed", labels: map[string]string{LabelState: "cordoned"},
			want: lifecycle{State: StateActive}, wantErr: "impala.ai/state",
		},
		{
			name: "draining without phase is malformed", labels: map[string]string{LabelState: "draining"},
			want: lifecycle{State: StateActive}, wantErr: "impala.ai/phase",
		},
		{
			name: "draining unknown phase is malformed", labels: map[string]string{LabelState: "draining", LabelPhase: "hold"},
			want: lifecycle{State: StateActive}, wantErr: "impala.ai/phase",
		},
		{
			name:   "migrate without budget is malformed",
			labels: map[string]string{LabelState: "draining", LabelPhase: "migrate"},
			want:   lifecycle{State: StateActive}, wantErr: "impala.ai/budget-tpm",
		},
		{
			name:   "migrate budget zero is malformed",
			labels: map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "0"},
			want:   lifecycle{State: StateActive}, wantErr: "impala.ai/budget-tpm",
		},
		{
			name:   "migrate budget non-integer is malformed",
			labels: map[string]string{LabelState: "draining", LabelPhase: "migrate", LabelBudgetTPM: "1e5"},
			want:   lifecycle{State: StateActive}, wantErr: "impala.ai/budget-tpm",
		},
		{
			name:   "malformed endpoint keeps its box",
			labels: map[string]string{LabelState: "bogus", LabelBox: "eu6"},
			want:   lifecycle{State: StateActive, Box: "eu6"}, wantErr: "impala.ai/state",
		},
		{
			name:   "stray phase/share/budget on an active endpoint are ignored",
			labels: map[string]string{LabelPhase: "migrate", LabelShare: "x", LabelBudgetTPM: "y"},
			want:   lifecycle{State: StateActive},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLifecycle(tt.labels)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// ParseFloat would accept most of these; the manager's share_str grammar,
// ^(0(\.[0-9]{1,4})?|1(\.0{1,4})?)$, does not.
func TestParseLifecycle_ShareOutsideShareStrGrammarIsMalformed(t *testing.T) {
	for _, share := range []string{"0x1p-2", "0x.8p0", "+0.5", "+1", ".5", "1.", "0.", "1e0", "5e-1", "0.12345", "1.00001", "1.0001", "-0", "-0.5", "NaN", "Inf", "+Inf", "1_0", "00.5", "01", " 0.5", ""} {
		t.Run(share, func(t *testing.T) {
			got, err := parseLifecycle(map[string]string{LabelState: "warming", LabelShare: share})
			require.ErrorContains(t, err, "impala.ai/share")
			assert.Equal(t, lifecycle{State: StateActive}, got)
		})
	}
}

// Rate-limit labels share the impala.ai/ prefix (impala-gateway spec 2026-10-13 §5.2); the
// admission parser must neither error on them nor change an endpoint's state because of them.
func TestParseLifecycle_IgnoresRateLimitLabels(t *testing.T) {
	lc, err := parseLifecycle(map[string]string{"impala.ai/limit-rpm": "30", "impala.ai/limit-tpm": "abc"})
	require.NoError(t, err)
	assert.Equal(t, StateActive, lc.State)
	lc, err = parseLifecycle(map[string]string{"impala.ai/state": "draining", "impala.ai/phase": "retain", "impala.ai/limit-rpm": "30"})
	require.NoError(t, err)
	assert.Equal(t, StateDraining, lc.State)
	assert.Equal(t, PhaseRetain, lc.Phase)
}
