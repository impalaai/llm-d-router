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

// Package impalaratelimit implements impala-rate-limit-filter (impala fork): a per-endpoint
// RPM / TPM overload guard. It measures each engine's throughput from the vLLM counters the
// EPP already polls (so every EPP replica sees the same engine-global rate) and drops an
// engine at its impala.ai/limit-* label from the scheduling candidates, keeping every
// candidate when all of them are over. Design: impala-gateway
// docs/superpowers/specs/2026-10-13-llmd-per-endpoint-rate-limits-design.md.
package impalaratelimit
