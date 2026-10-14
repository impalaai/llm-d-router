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
	"strings"

	dto "github.com/prometheus/client_model/go"

	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

// vLLM families as metrics-data-source parses the text exposition: a counter's family name
// carries its _total suffix (the Prometheus text format), a histogram's is the base name.
const (
	familyRequests     = "vllm:time_to_first_token_seconds" // histogram; sample count = requests that produced a first token
	familyPrompt       = "vllm:prompt_tokens_total"         // all prompt tokens, cached included
	familyGenerated    = "vllm:generation_tokens_total"
	familyLocalHits    = "vllm:prefix_cache_hits_total"          // tokens served from the engine's own prefix cache
	familyExternalHits = "vllm:external_prefix_cache_hits_total" // KV-connector engines only (Mooncake, LMCache)
)

// counters is one poll's engine totals, each summed over every series of its family.
type counters struct {
	Requests, Prompt, Generated, LocalHits, ExternalHits                float64
	HasRequests, HasPrompt, HasGenerated, HasLocalHits, HasExternalHits bool
}

func readCounters(families sourcemetrics.PrometheusMetricMap) counters {
	var c counters
	c.Requests, c.HasRequests = sumFamily(families, familyRequests)
	c.Prompt, c.HasPrompt = sumFamily(families, familyPrompt)
	c.Generated, c.HasGenerated = sumFamily(families, familyGenerated)
	c.LocalHits, c.HasLocalHits = sumFamily(families, familyLocalHits)
	c.ExternalHits, c.HasExternalHits = sumFamily(families, familyExternalHits)
	return c
}

// sumFamily adds up every series of a family, so per-`engine` (data parallel) and other label
// splits all count. A family missing under its name is looked up without the _total suffix
// (an exporter whose counter SAMPLE name lacks _total, e.g. OpenMetrics-style).
func sumFamily(families sourcemetrics.PrometheusMetricMap, name string) (float64, bool) {
	f, ok := families[name]
	if !ok || f == nil {
		f, ok = families[strings.TrimSuffix(name, "_total")]
	}
	if !ok || f == nil || len(f.GetMetric()) == 0 {
		return 0, false
	}
	var total float64
	for _, m := range f.GetMetric() {
		total += value(f.GetType(), m)
	}
	return total, true
}

func value(t dto.MetricType, m *dto.Metric) float64 {
	switch t {
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue()
	case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
		return float64(m.GetHistogram().GetSampleCount())
	case dto.MetricType_GAUGE:
		return m.GetGauge().GetValue()
	default:
		return m.GetUntyped().GetValue()
	}
}
