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
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
)

// Metric names are the impala-gateway CONTRACT (spec §4.6): bare impala_* names, no
// subsystem. `endpoint` is EndpointMetadata.ID.Name (file-discovery's entry `name`).
const (
	labelEndpoint  = "endpoint"
	labelDimension = "dimension"
	dimRPM         = "rpm"
	dimTPM         = "tpm"
)

var (
	engineRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_rate_limit_engine_rate",
		Help: metricsutil.HelpMsgWithStability("Measured engine throughput per minute over the plugin's window: requests (rpm) or uncached prompt + generated tokens (tpm). Absent while unmeasured.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, labelDimension})

	limitGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_rate_limit_limit",
		Help: metricsutil.HelpMsgWithStability("Configured per-minute limit from the endpoint's impala.ai/limit-* label (absent when none).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, labelDimension})

	unmeasuredGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_rate_limit_unmeasured",
		Help: metricsutil.HelpMsgWithStability("1 while a limited dimension has no measurement (warm-up, failing polls, missing vLLM family) and so cannot limit; 0 otherwise.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, labelDimension})

	overGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_rate_limit_over",
		Help: metricsutil.HelpMsgWithStability("1 while the measured rate is at or above the limit, 0 otherwise.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, labelDimension})

	droppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "impala_rate_limit_dropped_total",
		Help: metricsutil.HelpMsgWithStability("Candidates the filter dropped because the engine was at a limit.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	failOpenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "impala_rate_limit_fail_open_total",
		Help: metricsutil.HelpMsgWithStability("Scheduling cycles in which every candidate was over a limit, so all were kept.", compbasemetrics.ALPHA),
	})

	labelErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "impala_rate_limit_label_errors_total",
		Help: metricsutil.HelpMsgWithStability("Malformed impala.ai/limit-* labels seen, per candidate evaluation (that dimension is not limited).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	panicsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "impala_rate_limit_panics_total",
		Help: metricsutil.HelpMsgWithStability("Panics recovered inside impala-rate-limit-filter (Filter passes its candidates through; a publish is skipped).", compbasemetrics.ALPHA),
	})
)

func allCollectors() []prometheus.Collector {
	return []prometheus.Collector{engineRate, limitGauge, unmeasuredGauge, overGauge,
		droppedTotal, failOpenTotal, labelErrorsTotal, panicsTotal}
}

// registerMetrics registers the collectors; a repeat registration of the same collector is
// tolerated and a nil registerer is a no-op (impalaadmission/metrics.go's pattern).
func registerMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return nil
	}
	for _, collector := range allCollectors() {
		if err := registerer.Register(collector); err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) && already.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("register impala rate-limit metric: %w", err)
		}
	}
	return nil
}

// deleteSeries drops every series of an endpoint that left the pool.
func deleteSeries(endpoint string) {
	match := prometheus.Labels{labelEndpoint: endpoint}
	engineRate.DeletePartialMatch(match)
	limitGauge.DeletePartialMatch(match)
	unmeasuredGauge.DeletePartialMatch(match)
	overGauge.DeletePartialMatch(match)
	droppedTotal.DeletePartialMatch(match)
	labelErrorsTotal.DeletePartialMatch(match)
}
