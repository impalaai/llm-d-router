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

package sessionaffinity

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

// Values of the result label of sharedPinResults.
const (
	// pinResultHit and pinResultMiss count reads that found a pin or none.
	pinResultHit  = "hit"
	pinResultMiss = "miss"
	// pinResultCreated and pinResultCreateLost count pins this replica stored
	// for a new session, and creates another replica won.
	pinResultCreated    = "created"
	pinResultCreateLost = "create_lost"
	// pinResultMoved and pinResultMoveLost count pins this replica moved off
	// an absent pod, and moves another replica won.
	pinResultMoved    = "moved"
	pinResultMoveLost = "move_lost"
	// pinResultError counts store calls that failed or timed out; the request
	// was routed on the local binding.
	pinResultError = "error"
)

// Values of the operation label of sharedPinCallDuration.
const (
	pinOperationRead   = "read"
	pinOperationCreate = "create"
	pinOperationMove   = "move"
)

var (
	sharedPinResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_affinity_shared_pin_total",
			Help:      metricsutil.HelpMsgWithStability("Outcomes of the shared session pin calls of the session_id strategy.", compbasemetrics.ALPHA),
		},
		[]string{"plugin_name", "plugin_type", "result"},
	)

	sharedPinCallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "session_affinity_shared_pin_call_duration_seconds",
			Help:      metricsutil.HelpMsgWithStability("Duration of the shared session pin store calls of the session_id strategy.", compbasemetrics.ALPHA),
			Buckets:   []float64{0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1},
		},
		[]string{"plugin_name", "plugin_type", "operation"},
	)
)

func registerMetrics(registerer prometheus.Registerer) error {
	for _, collector := range []prometheus.Collector{sharedPinResults, sharedPinCallDuration} {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("register session affinity metric: %w", err)
		}
	}
	return nil
}

// recordPinCall records one store call and its outcome.
func (s *SessionIDHeader) recordPinCall(operation, result string, start time.Time) {
	sharedPinCallDuration.WithLabelValues(s.typedName.Name, s.typedName.Type, operation).Observe(time.Since(start).Seconds())
	sharedPinResults.WithLabelValues(s.typedName.Name, s.typedName.Type, result).Inc()
}
