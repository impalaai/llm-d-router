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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/cross_plugin/redis"
)

const (
	// integrationParamsEnv holds the redis-syncer JSON parameters for
	// TestIntegrationSharedPins. The redis-syncer integration test reads the
	// same variable.
	integrationParamsEnv = "REDIS_SYNCER_IT_PARAMS"
	// integrationPinTTLSeconds is the lifetime of the pins the test stores.
	// The test deletes nothing: its keys expire.
	integrationPinTTLSeconds = 60
	integrationReplicas      = 3
	integrationSessions      = 300
	// integrationParallelSessions is how many sessions are in flight at once,
	// each on every replica.
	integrationParallelSessions = 16
	// integrationStartup stands for the time between the creation of an EPP's
	// plugins and its first request. The syncer connects during it.
	integrationStartup = time.Second
)

// TestIntegrationSharedPins runs several session_id strategy instances, each
// with its own redis-syncer client, against a real Redis or Redis Cluster. It
// skips unless REDIS_SYNCER_IT_PARAMS holds the redis-syncer JSON parameters:
//
//	REDIS_SYNCER_IT_PARAMS='{"address":"127.0.0.1:7001","cluster":true,"tls":{},"passwordFile":"/tmp/redis-password"}' \
//	  go test ./pkg/epp/framework/plugins/scheduling/util/sessionaffinity/ -run TestIntegrationSharedPins -count=1 -v
//
// The test overrides keyPrefix with a unique it-<uuid> prefix.
func TestIntegrationSharedPins(t *testing.T) {
	raw := os.Getenv(integrationParamsEnv)
	if raw == "" {
		t.Skipf("set %s to run against a real Redis", integrationParamsEnv)
	}
	params := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &params))
	run := uuid.NewString()
	params["keyPrefix"] = "it-" + run
	encoded, err := json.Marshal(params)
	require.NoError(t, err)

	newReplica := func() *pinReplica {
		syncer, err := redis.RedisSyncerFactory("redis", json.NewDecoder(bytes.NewReader(encoded)), nil)
		require.NoError(t, err)
		return newPinReplica(t, syncer, func(c *SessionIDConfig) { c.EvictionTTLSeconds = integrationPinTTLSeconds })
	}
	replicas := make([]*pinReplica, integrationReplicas)
	for i := range replicas {
		replicas[i] = newReplica()
	}
	sessions := make([]string, integrationSessions)
	for i := range sessions {
		sessions[i] = fmt.Sprintf("%s-%d", run, i)
	}
	time.Sleep(integrationStartup)

	pinned := make([]string, len(sessions))

	t.Run("a new session on every replica at once gets one pod", func(t *testing.T) {
		before := pinResults()
		routed, durations := serveOnAll(replicas, "new", sessions,
			func(int) []string { return testPods },
			func(_, replica int) string { return testPods[replica] })
		delta := logPinResults(t, "new sessions", before, durations)

		require.Zero(t, delta[pinResultError], "store calls that failed or timed out")
		requireOnePodPerSession(t, replicas, sessions, routed)
		require.EqualValues(t, len(sessions), delta[pinResultCreated], "one create wins per session")
		for i := range sessions {
			pinned[i] = routed[i][0]
		}
	})

	t.Run("a restarted replica routes to the stored pins", func(t *testing.T) {
		before := pinResults()
		restarted := []*pinReplica{newReplica()}
		time.Sleep(integrationStartup)
		routed, durations := serveOnAll(restarted, "restart", sessions,
			func(int) []string { return testPods },
			func(session, _ int) string { return otherPods(pinned[session])[0] })
		delta := logPinResults(t, "restarted replica", before, durations)

		require.Zero(t, delta[pinResultError], "store calls that failed or timed out")
		for i := range sessions {
			require.Equal(t, pinned[i], routed[i][0], "session %d left its stored pod", i)
		}
	})

	t.Run("a session whose pod left moves to one pod on every replica", func(t *testing.T) {
		before := pinResults()
		routed, durations := serveOnAll(replicas, "move", sessions,
			func(session int) []string { return otherPods(pinned[session]) },
			func(session, replica int) string { return otherPods(pinned[session])[min(replica, 1)] })
		delta := logPinResults(t, "moved sessions", before, durations)

		require.Zero(t, delta[pinResultError], "store calls that failed or timed out")
		requireOnePodPerSession(t, replicas, sessions, routed)
		require.EqualValues(t, len(sessions), delta[pinResultMoved], "one move wins per session")
		for i := range sessions {
			require.NotEqual(t, pinned[i], routed[i][0], "session %d stayed on the pod that left", i)
		}
	})
}

// serveRevised runs one request of sessionID through Choose, ReviseTargets and
// PreRequest and returns the pod it is routed to. pick is the pod the
// scheduler picks when Choose pins none.
func (r *pinReplica) serveRevised(requestID, sessionID, pick string, candidates []string) string {
	request := requestWithSession(sessionID)
	request.RequestID = requestID
	endpoints := make([]scheduling.Endpoint, 0, len(candidates))
	for _, pod := range candidates {
		endpoints = append(endpoints, r.endpoints[pod])
	}
	if target, ok := r.header.Choose(context.Background(), request, endpoints); ok && target != nil {
		pick = target.GetMetadata().ID.Name
	}
	result := schedulingResultFor(r.endpoints[pick])
	profile := result.ProfileResults[result.PrimaryProfileName]
	for _, endpoint := range endpoints {
		profile.ScoredCandidates = append(profile.ScoredCandidates, scheduling.ScoredEndpoint{Endpoint: endpoint})
	}
	r.header.ReviseTargets(context.Background(), request, result)
	r.header.PreRequest(context.Background(), request, result)
	return profile.TargetEndpoints[0].GetMetadata().ID.Name
}

// serveOnAll serves one request of every session on every replica, the
// replicas of one session starting together. It returns the pod each replica
// routed each session to, and how long each request spent in the strategy.
func serveOnAll(replicas []*pinReplica, round string, sessions []string, candidates func(session int) []string, pick func(session, replica int) string) ([][]string, []time.Duration) {
	routed := make([][]string, len(sessions))
	durations := make([]time.Duration, len(sessions)*len(replicas))
	slots := make(chan struct{}, integrationParallelSessions)
	var all sync.WaitGroup
	for i, session := range sessions {
		routed[i] = make([]string, len(replicas))
		slots <- struct{}{}
		all.Go(func() {
			defer func() { <-slots }()
			start := make(chan struct{})
			var one sync.WaitGroup
			for j, replica := range replicas {
				one.Go(func() {
					<-start
					began := time.Now()
					routed[i][j] = replica.serveRevised(fmt.Sprintf("%s-%s-%d", round, session, j), session, pick(i, j), candidates(i))
					durations[i*len(replicas)+j] = time.Since(began)
				})
			}
			close(start)
			one.Wait()
		})
	}
	all.Wait()
	return routed, durations
}

// requireOnePodPerSession fails when the replicas routed a session to
// different pods or hold different local bindings for it.
func requireOnePodPerSession(t *testing.T, replicas []*pinReplica, sessions []string, routed [][]string) {
	t.Helper()
	split := 0
	for i, session := range sessions {
		agreed := true
		for j, replica := range replicas {
			if routed[i][j] != routed[i][0] || replica.header.boundPod(session) != podKey(routed[i][0]) {
				agreed = false
			}
		}
		if !agreed {
			split++
			if split <= 5 {
				t.Logf("session %d: routed %v", i, routed[i])
			}
		}
	}
	require.Zero(t, split, "sessions the replicas disagree on")
}

func otherPods(pod string) []string {
	return slices.DeleteFunc(slices.Clone(testPods), func(p string) bool { return p == pod })
}

var integrationPinResults = []string{
	pinResultHit, pinResultMiss, pinResultCreated, pinResultCreateLost,
	pinResultMoved, pinResultMoveLost, pinResultRedirected, pinResultError,
}

func pinResults() map[string]float64 {
	counts := map[string]float64{}
	for _, result := range integrationPinResults {
		counts[result] = testutil.ToFloat64(sharedPinResults.WithLabelValues(testTypedName.Name, testTypedName.Type, result))
	}
	return counts
}

// logPinResults logs and returns the shared-pin outcomes counted since before,
// with the spread of durations when given.
func logPinResults(t *testing.T, phase string, before map[string]float64, durations []time.Duration) map[string]float64 {
	t.Helper()
	delta := pinResults()
	var line strings.Builder
	line.WriteString(phase + ":")
	for _, result := range integrationPinResults {
		delta[result] -= before[result]
		fmt.Fprintf(&line, " %s=%.0f", result, delta[result])
	}
	t.Log(line.String())
	if len(durations) > 0 {
		slices.Sort(durations)
		t.Logf("%s: time per request in the strategy: p50=%s p99=%s max=%s",
			phase, durations[len(durations)/2], durations[len(durations)*99/100], durations[len(durations)-1])
	}
	return delta
}
