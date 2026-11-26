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
	"context"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// defaultStoreTimeoutSeconds bounds the shared-pin calls of one request.
const defaultStoreTimeoutSeconds = 0.1

// sharedPinID keys a session in the shared store so that session identifiers
// do not leave the process.
func sharedPinID(sessionID string) string {
	return strconv.FormatUint(xxhash.Sum64String(sessionID), 36)
}

// pinStore returns the store shared pins are kept in, or nil when pins stay
// local to this replica: syncCrossReplicaState is off, no cross-replica syncer
// is configured, or the syncer cannot store per-key values.
func (s *SessionIDHeader) pinStore(ctx context.Context) datalayer.CrossReplicaValueStore {
	if !s.syncCrossReplicaState {
		return nil
	}
	syncer := s.handle.CrossReplicaSyncer()
	if syncer == nil {
		return nil
	}
	store, ok := syncer.(datalayer.CrossReplicaValueStore)
	if !ok {
		s.warnNoStore.Do(func() {
			log.FromContext(ctx).Info("Session affinity - the cross-replica syncer cannot store per-key values; session bindings stay local",
				"plugin", string(s.pluginKey), "syncer", syncer.TypedName().String())
		})
		return nil
	}
	return store
}

// loadSharedPin reads the shared pin of sessionID and resets its expiration.
// ok is false when the store is not in use or did not answer in time; the
// caller then routes on the local binding alone.
func (s *SessionIDHeader) loadSharedPin(ctx context.Context, sessionID string) (podName string, ok bool) {
	store := s.pinStore(ctx)
	if store == nil {
		return "", false
	}
	ctx, cancel := context.WithTimeout(ctx, s.storeTimeout)
	defer cancel()
	start := time.Now()
	podName, found, err := store.LoadAndRefresh(ctx, s.pinStateKey, sharedPinID(sessionID), s.ttl)
	if err != nil {
		s.recordPinCall(pinOperationRead, pinResultError, start)
		log.FromContext(ctx).V(logging.DEFAULT).Info("Session affinity - shared pin read failed, using the local binding",
			"plugin", string(s.pluginKey), "error", err.Error())
		return "", false
	}
	if !found {
		s.recordPinCall(pinOperationRead, pinResultMiss, start)
		return "", true
	}
	s.recordPinCall(pinOperationRead, pinResultHit, start)
	return podName, true
}

// commitSharedPin settles the shared pin of a request routed to podName and
// returns the pod the session is pinned to afterwards. presence is what this
// request's Choose read from the store. ok is false when the store did not
// answer; the caller then commits the local binding alone.
//
// A session without a shared pin is pinned to podName unless another replica
// pinned it first. A pin moves to podName only when its pod was absent from
// the request's candidates and no other replica moved it first.
func (s *SessionIDHeader) commitSharedPin(ctx context.Context, sessionID, podName string, presence *boundPodPresence) (pinned string, ok bool) {
	store := s.pinStore(ctx)
	if store == nil {
		return "", false
	}
	if presence.sharedPod == podName || (presence.sharedPod != "" && presence.present) {
		return presence.sharedPod, true
	}
	ctx, cancel := context.WithTimeout(ctx, s.storeTimeout)
	defer cancel()
	operation, won, lost := pinOperationCreate, pinResultCreated, pinResultCreateLost
	start := time.Now()
	var err error
	var stored bool
	if presence.sharedPod == "" {
		var loaded bool
		pinned, loaded, err = store.LoadOrStore(ctx, s.pinStateKey, sharedPinID(sessionID), podName, s.ttl)
		stored = !loaded
	} else {
		operation, won, lost = pinOperationMove, pinResultMoved, pinResultMoveLost
		pinned, stored, err = store.CompareAndSwap(ctx, s.pinStateKey, sharedPinID(sessionID), presence.sharedPod, podName, s.ttl)
	}
	if err != nil {
		s.recordPinCall(operation, pinResultError, start)
		log.FromContext(ctx).V(logging.DEFAULT).Info("Session affinity - shared pin write failed, binding locally",
			"plugin", string(s.pluginKey), "error", err.Error())
		return "", false
	}
	if stored {
		s.recordPinCall(operation, won, start)
	} else {
		s.recordPinCall(operation, lost, start)
	}
	return pinned, true
}

// ReviseTargets settles the shared pin of a scheduled request before it is
// routed. When another replica pinned the session to a different pod in the
// meantime, the request is routed to that pod, provided the profile scored it.
// It implements requesthandling.TargetReviser for the owning plugin.
func (s *SessionIDHeader) ReviseTargets(ctx context.Context, request *scheduling.InferenceRequest, schedulingResult *scheduling.SchedulingResult) {
	sessionID := s.resolveSessionID(ctx, request)
	if sessionID == "" {
		return
	}
	podName := s.pickedPodName(schedulingResult)
	if podName == "" {
		return
	}
	presence, err := plugin.ReadPluginStateKey[*boundPodPresence](s.pluginState, request.RequestID, s.pluginKey)
	if err != nil || presence == nil || !presence.shared {
		return
	}
	pinned, ok := s.commitSharedPin(ctx, sessionID, podName, presence)
	settled := *presence
	if !ok {
		// PreRequest then binds locally and does not repeat the failed write.
		settled.shared = false
		s.pluginState.Write(request.RequestID, s.pluginKey, &settled)
		return
	}
	settled.settled, settled.settledPod = true, pinned
	s.pluginState.Write(request.RequestID, s.pluginKey, &settled)
	// A pin equal to the one Choose read was already an input to scheduling.
	if pinned == podName || pinned == presence.sharedPod {
		return
	}

	profile := s.profileResult(schedulingResult)
	var winner scheduling.Endpoint
	for i := range profile.ScoredCandidates {
		if candidate := profile.ScoredCandidates[i].Endpoint; candidate != nil && candidate.GetMetadata().ID.String() == pinned {
			winner = candidate
			break
		}
	}
	if winner == nil {
		log.FromContext(ctx).V(logging.DEFAULT).Info("Session affinity - the pinned pod was not scored for this request, keeping the picked pod",
			"plugin", string(s.pluginKey), "pinned", pinned, "picked", podName)
		return
	}
	targets := []scheduling.Endpoint{winner}
	for _, target := range profile.TargetEndpoints {
		if target != nil && target.GetMetadata().ID.String() != pinned {
			targets = append(targets, target)
		}
	}
	profile.TargetEndpoints = targets[:len(profile.TargetEndpoints)]
	sharedPinResults.WithLabelValues(s.typedName.Name, s.typedName.Type, pinResultRedirected).Inc()
	log.FromContext(ctx).V(logging.DEFAULT).Info("Session affinity - routing to the pod another replica pinned",
		"plugin", string(s.pluginKey), "pinned", pinned, "picked", podName)
}

// bindLocal records podName as the local binding of sessionID.
func (s *SessionIDHeader) bindLocal(sessionID, podName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings.Store(sessionID, binding{podName: podName, lastSeen: time.Now()})
}
