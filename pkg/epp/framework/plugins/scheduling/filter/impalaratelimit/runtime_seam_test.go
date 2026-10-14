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
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

// TestRuntimeSeam_TheRealMetricsSourceFeedsTheExtractor drives the seam the feature hangs on:
// RegisterDependencies -> the datalayer runtime binds the plugin to the real metrics data
// source -> a real HTTP scrape is parsed -> Extract stores samples -> the engine becomes measured.
// A wrong SourceType in RegisterDependencies leaves the plugin unbound and this test times out.
// It runs on the real clock because a dimension needs samples spanning half the (minimum) 10 s
// window, so it takes about 6 s.
func TestRuntimeSeam_TheRealMetricsSourceFeedsTheExtractor(t *testing.T) {
	if testing.Short() {
		t.Skip("takes ~6 s: a measurement needs samples spanning half the minimum window")
	}
	resetMetrics()
	body, err := os.ReadFile("testdata/glm-5.3.txt")
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	p := newPlugin("rl", Parameters{WindowSeconds: 10}, time.Now)
	src, err := sourcemetrics.NewHTTPMetricsDataSource("http", "/metrics", sourcemetrics.MetricsDataSourceType)
	require.NoError(t, err)

	rt := datalayer.NewRuntime(50 * time.Millisecond)
	require.NoError(t, p.RegisterDependencies(rt))
	require.NoError(t, rt.Configure(&datalayer.Config{
		Sources: []datalayer.DataSourceConfig{{Plugin: src}}, // no extractor of its own: only the plugin's registration binds one
	}, logr.Discard()))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ep := rt.NewEndpoint(ctx, &fwkdl.EndpointMetadata{
		ID:          k8stypes.NamespacedName{Namespace: "default", Name: "a"},
		Name:        "a",
		MetricsHost: srv.Listener.Addr().String(),
		Port:        "8000",
		Labels:      map[string]string{LabelLimitRPM: "1000", LabelLimitTPM: "1000000"},
	})
	require.NotNil(t, ep)
	t.Cleanup(func() { rt.ReleaseEndpoint(ep) })

	// The static fixture has a constant counter, so the measured rate is 0: what proves the seam is
	// that the series EXISTS (engine_rate is absent while unmeasured), for both dimensions.
	require.Eventually(t, func() bool {
		p.publishOnce(ids("a"))
		rates := series(t, engineRate)
		_, rpm := rates[dimRPM]
		_, tpm := rates[dimTPM]
		return rpm && tpm
	}, 20*time.Second, 250*time.Millisecond, "engine_rate was never published: the extractor is not receiving the metrics payload")
	require.Equal(t, 0.0, series(t, engineRate)[dimRPM])
	require.Equal(t, map[string]float64{dimRPM: 0, dimTPM: 0}, series(t, unmeasuredGauge))
}
