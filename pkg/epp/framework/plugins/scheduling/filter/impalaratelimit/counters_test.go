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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

// parseText parses Prometheus text exposition exactly as metrics-data-source does
// (source/metrics/datasource.go parseMetrics).
func parseText(t *testing.T, text string) sourcemetrics.PrometheusMetricMap {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation) // addressable: TextToMetricFamilies has a pointer receiver
	fams, err := parser.TextToMetricFamilies(strings.NewReader(text))
	require.NoError(t, err)
	return fams
}

// vllmText renders the four required families for one engine (one series each).
func vllmText(requests, prompt, generated, hits float64) string {
	return fmt.Sprintf(`# HELP vllm:time_to_first_token_seconds Histogram of time to first token in seconds.
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{engine="0",le="+Inf",model_name="m"} %[1]f
vllm:time_to_first_token_seconds_sum{engine="0",model_name="m"} 1
vllm:time_to_first_token_seconds_count{engine="0",model_name="m"} %[1]f
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{engine="0",model_name="m"} %[2]f
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{engine="0",model_name="m"} %[3]f
# TYPE vllm:prefix_cache_hits_total counter
vllm:prefix_cache_hits_total{engine="0",model_name="m"} %[4]f
`, requests, prompt, generated, hits)
}

func TestReadCounters_OneEngine(t *testing.T) {
	c := readCounters(parseText(t, vllmText(7, 1000, 200, 600)))
	assert.Equal(t, counters{
		Requests: 7, Prompt: 1000, Generated: 200, LocalHits: 600,
		HasRequests: true, HasPrompt: true, HasGenerated: true, HasLocalHits: true,
	}, c)
}

func TestReadCounters_SumsEveryDataParallelEngineSeries(t *testing.T) {
	text := `# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{engine="0",model_name="m"} 100
vllm:prompt_tokens_total{engine="1",model_name="m"} 250
# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{engine="0",le="+Inf",model_name="m"} 3
vllm:time_to_first_token_seconds_sum{engine="0",model_name="m"} 1
vllm:time_to_first_token_seconds_count{engine="0",model_name="m"} 3
vllm:time_to_first_token_seconds_bucket{engine="1",le="+Inf",model_name="m"} 4
vllm:time_to_first_token_seconds_sum{engine="1",model_name="m"} 1
vllm:time_to_first_token_seconds_count{engine="1",model_name="m"} 4
`
	c := readCounters(parseText(t, text))
	assert.Equal(t, 350.0, c.Prompt)
	assert.Equal(t, 7.0, c.Requests)
	assert.False(t, c.HasGenerated)
	assert.False(t, c.HasLocalHits)
}

func TestReadCounters_ExternalHitsAreOptional(t *testing.T) {
	c := readCounters(parseText(t, vllmText(1, 1, 1, 1)))
	assert.False(t, c.HasExternalHits)
	c = readCounters(parseText(t, vllmText(1, 1, 1, 1)+`# TYPE vllm:external_prefix_cache_hits_total counter
vllm:external_prefix_cache_hits_total{engine="0",model_name="m"} 42
`))
	assert.True(t, c.HasExternalHits)
	assert.Equal(t, 42.0, c.ExternalHits)
}

func TestReadCounters_FindsAFamilyWhoseTypeLineDropsTotal(t *testing.T) {
	text := `# TYPE vllm:prompt_tokens counter
vllm:prompt_tokens{engine="0",model_name="m"} 900
`
	c := readCounters(parseText(t, text))
	assert.True(t, c.HasPrompt)
	assert.Equal(t, 900.0, c.Prompt)
}

func TestReadCounters_EmptyPayload(t *testing.T) {
	assert.Equal(t, counters{}, readCounters(nil))
	assert.Equal(t, counters{}, readCounters(sourcemetrics.PrometheusMetricMap{}))
}

// Every captured live fixture (Task 0) must yield the four required families.
func TestReadCounters_LiveFixtures(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "*.txt"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "Task 0 must have captured at least one fixture")
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			require.NoError(t, err)
			c := readCounters(parseText(t, string(raw)))
			assert.True(t, c.HasRequests, "requests family")
			assert.True(t, c.HasPrompt, "prompt family")
			assert.True(t, c.HasGenerated, "generated family")
			assert.True(t, c.HasLocalHits, "local hits family")
			assert.GreaterOrEqual(t, c.Prompt, c.LocalHits, "prompt tokens include cached ones, so hits never exceed them")
		})
	}
}
