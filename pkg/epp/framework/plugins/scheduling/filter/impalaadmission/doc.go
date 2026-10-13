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

// Package impalaadmission provides the impala-admission-filter scheduling
// filter: a per-endpoint lifecycle gate driven by impala.ai/* endpoint labels
// (warming / draining-retain / draining-migrate) so an engine can join or leave
// a pool without a step change in prefix-cache hit rate. The decision rules
// are the reference implementation admission_filter in impala-gateway
// scripts/llmd-router/lifecycle-sim/sim.py (spec
// docs/superpowers/specs/2026-10-12-llmd-gradual-endpoint-lifecycle-design.md
// §5.3 as amended in §12); decide.go ports them one-to-one.
package impalaadmission
