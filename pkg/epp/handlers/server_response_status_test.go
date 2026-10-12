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

package handlers

import (
	"context"
	"io"
	"testing"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

// replayProcessServer feeds Process a fixed sequence of requests and then io.EOF, so
// Process returns nil and its deferred metrics block runs. v0.9.0's mockProcessServer
// (server_abort_test.go) cannot be reused: its Recv returns a sentinel error, which
// Process reports as a stream failure.
type replayProcessServer struct {
	ctx  context.Context
	reqs []*extProcPb.ProcessingRequest
}

func (r *replayProcessServer) Recv() (*extProcPb.ProcessingRequest, error) {
	if len(r.reqs) == 0 {
		return nil, io.EOF
	}
	req := r.reqs[0]
	r.reqs = r.reqs[1:]
	return req, nil
}
func (r *replayProcessServer) Send(*extProcPb.ProcessingResponse) error { return nil }
func (r *replayProcessServer) Context() context.Context                 { return r.ctx }
func (r *replayProcessServer) SetHeader(metadata.MD) error              { return nil }
func (r *replayProcessServer) SendHeader(metadata.MD) error             { return nil }
func (r *replayProcessServer) SetTrailer(metadata.MD)                   {}
func (r *replayProcessServer) SendMsg(any) error                        { return nil }
func (r *replayProcessServer) RecvMsg(any) error                        { return nil }

// Backport of upstream llm-d/llm-d-router#3330 (fork 0b08b74e) onto v0.9.0: Envoy
// delivers the response status as the ":status" pseudo-header, so behind Envoy a non-200
// model-server response never set ModelServerError and llm_d_epp_request_error_total
// never counted it.
func TestProcessCountsModelServerErrorFromResponseStatus(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantCount float64
	}{
		{name: "Envoy pseudo-header with an error status", key: ":status", value: "502", wantCount: 1},
		{name: "Envoy pseudo-header with 200", key: ":status", value: "200", wantCount: 0},
		{name: "plain status header with an error status", key: "status", value: "502", wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eppmetrics.Register()
			eppmetrics.Reset()
			t.Cleanup(eppmetrics.Reset)

			srv := &replayProcessServer{
				ctx: logutil.NewTestLoggerIntoContext(context.Background()),
				reqs: []*extProcPb.ProcessingRequest{{
					Request: &extProcPb.ProcessingRequest_ResponseHeaders{
						ResponseHeaders: &extProcPb.HttpHeaders{
							Headers: &configPb.HeaderMap{Headers: []*configPb.HeaderValue{
								{Key: tt.key, RawValue: []byte(tt.value)},
							}},
						},
					},
				}},
			}
			registry := NewParserRegistry([]fwkrh.Parser{openai.NewOpenAIParser()}, logr.Discard())

			require.NoError(t, NewStreamingServer(nil, &mockDirector{}, registry, 0).Process(srv))

			require.Equal(t, tt.wantCount, requestErrorCount(t, errcommon.ModelServerError))
		})
	}
}

// requestErrorCount sums llm_d_epp_request_error_total over every series with the given
// error code.
func requestErrorCount(t *testing.T, code string) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	var total float64
	for _, family := range families {
		if family.GetName() != "llm_d_epp_request_error_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "error_code" && l.GetValue() == code {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}
