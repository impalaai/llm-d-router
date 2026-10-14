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

package impalaratelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLimits(t *testing.T) {
	cases := []struct {
		name    string
		labels  map[string]string
		want    limits
		wantErr string
	}{
		{name: "none", labels: nil, want: limits{}},
		{name: "unrelated labels", labels: map[string]string{"impala.ai/box": "a", "impala.ai/state": "draining"}, want: limits{}},
		{name: "rpm only", labels: map[string]string{LabelLimitRPM: "30"}, want: limits{RPM: 30}},
		{name: "tpm only", labels: map[string]string{LabelLimitTPM: "3000000"}, want: limits{TPM: 3000000}},
		{name: "both", labels: map[string]string{LabelLimitRPM: "30", LabelLimitTPM: "2000000"}, want: limits{RPM: 30, TPM: 2000000}},
		{name: "upper bound", labels: map[string]string{LabelLimitTPM: "999999999"}, want: limits{TPM: 999999999}},
		{name: "zero is malformed", labels: map[string]string{LabelLimitRPM: "0"}, want: limits{}, wantErr: LabelLimitRPM},
		{name: "negative", labels: map[string]string{LabelLimitRPM: "-1"}, want: limits{}, wantErr: LabelLimitRPM},
		{name: "above bound", labels: map[string]string{LabelLimitTPM: "1000000000"}, want: limits{}, wantErr: LabelLimitTPM},
		{name: "exponent form", labels: map[string]string{LabelLimitTPM: "1e+06"}, want: limits{}, wantErr: LabelLimitTPM},
		{name: "decimal", labels: map[string]string{LabelLimitRPM: "30.5"}, want: limits{}, wantErr: LabelLimitRPM},
		{name: "empty", labels: map[string]string{LabelLimitRPM: ""}, want: limits{}, wantErr: LabelLimitRPM},
		{
			name:    "a malformed dimension does not disable the other",
			labels:  map[string]string{LabelLimitRPM: "abc", LabelLimitTPM: "5000"},
			want:    limits{TPM: 5000},
			wantErr: LabelLimitRPM,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLimits(tc.labels)
			assert.Equal(t, tc.want, got)
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestLimitsNone(t *testing.T) {
	assert.True(t, limits{}.none())
	assert.False(t, limits{RPM: 1}.none())
	assert.False(t, limits{TPM: 1}.none())
}
