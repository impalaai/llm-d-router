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

package redis

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func TestKeyPrefixAppliesToEveryKey(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.keyPrefix = "llmd:wix"
	spec := fwkdl.CrossReplicaSpec{
		StateKey:  testStateKey,
		Read:      func(string) fwkdl.Cloneable { return cloneableInt(1) },
		Aggregate: sumCloneableInts,
	}

	require.NoError(t, syncer.Set(t.Context(), spec, testEndpointID))
	require.Empty(t, syncer.SetBatch(t.Context(), spec, []string{"endpoint-a", "endpoint-b"}, time.Second))
	_, _, err := syncer.GetOrSet(t.Context(), "request", "request-id", "candidate")
	require.NoError(t, err)
	require.NoError(t, syncer.Delete(t.Context(), testStateKey, "endpoint-a"))

	keys := server.Keys()
	require.NotEmpty(t, keys)
	for _, key := range keys {
		require.True(t, strings.HasPrefix(key, "llmd:wix:"), "key %q lacks the prefix", key)
	}
	require.Equal(t, "llmd:wix:inflight:ep", syncer.hashKey("inflight", "ep"))
	require.Equal(t, "llmd:wix:coord:request:id", syncer.coordKey("request", "id"))
}

func TestKeyPrefixIsolatesPools(t *testing.T) {
	server := miniredis.RunT(t)
	wix, _ := newTestSyncer(t, server, "epp-wix")
	wix.keyPrefix = "llmd:wix"
	shadow, _ := newTestSyncer(t, server, "epp-shadow")
	shadow.keyPrefix = "llmd:wixshadow"
	wixPeer, _ := newTestSyncer(t, server, "epp-wix-2")
	wixPeer.keyPrefix = "llmd:wix"

	_, existed, err := wix.GetOrSet(t.Context(), "request", "request-id", "wix")
	require.NoError(t, err)
	require.False(t, existed)

	actual, existed, err := shadow.GetOrSet(t.Context(), "request", "request-id", "shadow")
	require.NoError(t, err)
	require.False(t, existed, "another pool's prefix must not see the value")
	require.Equal(t, "shadow", actual)

	actual, existed, err = wixPeer.GetOrSet(t.Context(), "request", "request-id", "wix-2")
	require.NoError(t, err)
	require.True(t, existed, "replicas sharing a prefix must share values")
	require.Equal(t, "wix", actual)
}

func TestEmptyKeyPrefixKeepsKeyNames(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)

	require.Equal(t, "inflight:ep", syncer.hashKey("inflight", "ep"))
	require.Equal(t, "coord:request:id", syncer.coordKey("request", "id"))
}

func TestFactoryReadsKeyPrefix(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "keyPrefix": "llmd:wix"}`, server.Addr()))

	require.Equal(t, "llmd:wix:coord:request:id", syncer.coordKey("request", "id"))
}
