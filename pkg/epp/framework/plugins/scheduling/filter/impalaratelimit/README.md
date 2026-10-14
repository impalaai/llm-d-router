# Impala Rate Limit Filter (impala fork)

**Type:** `impala-rate-limit-filter` — stability Alpha (`--allow-experimental-plugins`).

A per-endpoint RPM / TPM **overload guard**. It measures each engine's throughput from the engine's own
vLLM counters, read off the `/metrics` polls the EPP already makes, so every EPP replica (and both router
colors) see the same engine-global rate — no per-replica split. An engine at its `impala.ai/limit-*` label
is dropped from the candidates; when every candidate is over, all are kept (fail open). Design: impala-gateway
`docs/superpowers/specs/2026-10-13-llmd-per-endpoint-rate-limits-design.md`.

## Wiring

Declare the plugin and reference it in the profile. Nothing else: it attaches itself as an extractor to the
`metrics-data-source` (`RegisterDependencies`, `IfMissing: Warn`). The config loader injects that source
whenever it is not already among `dataLayer.sources` and `dataLayer.injectDefaults` is not `false`
(`pkg/epp/config/loader/defaults.go`, `ensureDataLayer`). With no `metrics-data-source` the plugin measures
nothing and never drops.

## Placement

After `session-affinity-filter` (if present) and after `impala-admission-filter` (if present), before the
scorers. A bound request's candidate list is its pinned engine alone, and the fail-open keeps it, so pins are
never broken; only unbound requests are steered.

## Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `windowSeconds` | int | `60` | Rolling window, `[10, 600]`. A dimension is measured once samples span half of it and the newest is at most a quarter of it old. |

## Endpoint labels

| Label | Value | Meaning |
|---|---|---|
| `impala.ai/limit-rpm` | integer `[1, 999999999]` | requests per minute (vLLM `time_to_first_token_seconds` count) |
| `impala.ai/limit-tpm` | integer `[1, 999999999]` | uncached prompt + generated tokens per minute |

TPM = `max(0, Δprompt_tokens − Δprefix_cache_hits − Δexternal_prefix_cache_hits) + Δgeneration_tokens` — the same
basis as LiteLLM's key/team TPM limiter (`total_tokens − cached_tokens`). A malformed label disables only its own
dimension and increments `impala_rate_limit_label_errors_total`. An engine without the hits family has TPM
unmeasured (never silently total tokens).

## Metrics (bare names, EPP :9090)

`impala_rate_limit_engine_rate{endpoint,dimension}`, `impala_rate_limit_limit{endpoint,dimension}`,
`impala_rate_limit_unmeasured{endpoint,dimension}`, `impala_rate_limit_over{endpoint,dimension}` (gauges,
republished every 5 s), `impala_rate_limit_dropped_total{endpoint}`, `impala_rate_limit_label_errors_total{endpoint}`,
`impala_rate_limit_fail_open_total{scope}`, `impala_rate_limit_panics_total`. `engine_rate` is published for every polled
engine, limited or not, while its dimension is measured (calibration data).

`fail_open_total{scope="pool"}` is the pool-saturation signal: every engine of the pool was over and all were
kept (the candidate count was at least the pool size last seen, or no pool size is known yet).
`scope="narrowed"` means a narrowed set was all over, typically a pinned session whose engine is over while
other engines may still have headroom; it does not indicate saturation.

A transition log line (INFO) marks an endpoint starting or ceasing to be limited: at most one line per endpoint
per 30 s, and the final state is always logged (a line suppressed by the cap is emitted at the next publish after
it expires, with a count of the transitions it covers).

## Example

```yaml
plugins:
  - type: impala-rate-limit-filter
    parameters: {windowSeconds: 60}
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: impala-rate-limit-filter
      - pluginRef: prefix-cache-scorer
        weight: 3
      - pluginRef: max-score-picker
```

## Fork provenance

Branch `impala/rate-limits` = `impala/v0.10.0-lifecycle` (`impala-admission-filter`) + this package.
