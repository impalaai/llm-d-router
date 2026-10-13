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

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
)

// Metric names are the impala-gateway lifecycle CONTRACT (plan 1 Global
// Constraints): bare impala_* names, no subsystem. `endpoint` is the
// endpoint's EndpointMetadata.ID.Name everywhere (Filter, ResponseHeader and
// the refresh loop alike), which file-discovery sets to the entry's `name`.
const labelEndpoint = "endpoint"

var (
	decisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "impala_admission_decisions_total",
		Help: metricsutil.HelpMsgWithStability("impala-admission-filter decisions per endpoint, lifecycle state/phase and outcome (retained|steered|moved|deferred|forced).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, "state", "phase", "decision"})

	movedTokensTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "impala_admission_moved_tokens_total",
		Help: metricsutil.HelpMsgWithStability("Prefix tokens the filter chose to re-prefill elsewhere by moving work off a draining endpoint.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	bucketTokens = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_admission_bucket_tokens",
		Help: metricsutil.HelpMsgWithStability("Migrate token-bucket level per draining endpoint on this EPP replica after its last take (negative while in debt).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	endpointLoad = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_admission_endpoint_load",
		Help: metricsutil.HelpMsgWithStability("Load measure the warming overload stop uses: recent session bindings on session planes, in-flight requests on prefix planes.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	sessionBindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_session_bindings",
		Help: metricsutil.HelpMsgWithStability("Unexpired session bindings per endpoint (session planes only).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	sessionBindingsRecent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "impala_session_bindings_recent",
		Help: metricsutil.HelpMsgWithStability("Session bindings per endpoint refreshed within recentBindingWindowSeconds (session planes only).", compbasemetrics.ALPHA),
	}, []string{labelEndpoint})

	endpointErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "impala_endpoint_errors_total",
		Help: metricsutil.HelpMsgWithStability("Model-server responses with an HTTP status in 400-599, by serving endpoint and status code.", compbasemetrics.ALPHA),
	}, []string{labelEndpoint, "code"})

	labelErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "impala_admission_label_errors_total",
		Help: metricsutil.HelpMsgWithStability("Malformed or unknown impala.ai/* label sets seen, counted per candidate evaluation (a persistent bad label counts once per candidate per request; treated as active).", compbasemetrics.ALPHA),
	})

	panicsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "impala_admission_panics_total",
		Help: metricsutil.HelpMsgWithStability("Panics recovered inside the impala-admission-filter (Filter passes its candidates through unchanged; a refresh is skipped).", compbasemetrics.ALPHA),
	})
)

func allCollectors() []prometheus.Collector {
	return []prometheus.Collector{decisionsTotal, movedTokensTotal, bucketTokens, endpointLoad,
		sessionBindings, sessionBindingsRecent, endpointErrorsTotal, labelErrorsTotal, panicsTotal}
}

// registerMetrics registers the collectors. A repeat registration of the same
// collector is tolerated (the pattern of approximateprefix/metrics.go
// @v0.10.0). Unlike that precedent a nil registerer is a no-op: the collectors
// still drive the plugin, they are just not exported.
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
			return fmt.Errorf("register impala admission metric: %w", err)
		}
	}
	return nil
}
