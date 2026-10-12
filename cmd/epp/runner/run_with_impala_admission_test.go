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

package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	runserver "github.com/llm-d/llm-d-router/pkg/epp/server"
)

// runImpalaAdmissionConfig boots runWithFileDiscovery on a config that declares
// an impala-admission-filter (sessionAffinityPluginName: ref) next to
// extraPlugins and returns the start-up error.
func runImpalaAdmissionConfig(t *testing.T, extraPlugins, ref string) error {
	t.Helper()
	dir := t.TempDir()
	endpointsPath := filepath.Join(dir, "endpoints.yaml")
	require.NoError(t, os.WriteFile(endpointsPath, []byte("endpoints: []\n"), 0o644))

	configText := fmt.Sprintf(`apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  - name: file-discovery
    type: file-discovery
    parameters:
      path: %q
      watchFile: false
%s  - type: impala-admission-filter
    parameters:
      sessionAffinityPluginName: %s
  - type: max-score-picker
dataLayer:
  injectDefaults: false
  discovery:
    pluginRef: file-discovery
schedulingProfiles:
  - name: default
    plugins:
    - pluginRef: impala-admission-filter
    - pluginRef: max-score-picker
`, endpointsPath, extraPlugins, ref)

	opts := runserver.NewOptions()
	opts.AllowExperimentalPlugins = true
	opts.ConfigText = configText

	r := NewRunner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawConfig, err := r.parseConfigurationPhaseOne(ctx, opts)
	require.NoError(t, err)
	return r.runWithFileDiscovery(ctx, opts, rawConfig)
}

// A sessionAffinityPluginName that no plugin declares fails config load, so a
// typo in the chart can never boot an EPP that silently runs without bindings.
func TestRunWithImpalaAdmission_DanglingRefFailsStartup(t *testing.T) {
	err := runImpalaAdmissionConfig(t, "", "not-declared")
	require.Error(t, err)
	require.Contains(t, err.Error(), "a plugin has a dependency on the unregistered plugin not-declared")
}

// A reference to a session-affinity-filter that does not run strategy
// session_id (the default is token) has no binding table to read, so it fails
// config load rather than degrading to a prefix-only plane.
func TestRunWithImpalaAdmission_NonSessionIDRefFailsStartup(t *testing.T) {
	err := runImpalaAdmissionConfig(t, "  - type: session-affinity-filter\n", "session-affinity-filter")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not a session-affinity-filter with strategy session_id")
}
