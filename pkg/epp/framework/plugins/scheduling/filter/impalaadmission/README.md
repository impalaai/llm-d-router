# Impala Admission Filter (impala fork)

**Type:** `impala-admission-filter` — stability Alpha (`--allow-experimental-plugins`).

Per-endpoint lifecycle gate driven by `impala.ai/*` endpoint labels so an engine can join or
leave a pool without a step change in prefix-cache hit rate. The EPP holds no schedule: it
applies the labels it currently sees; timing lives in the llmd-manager. Design: impala-gateway
`docs/superpowers/specs/2026-10-12-llmd-gradual-endpoint-lifecycle-design.md` (§5); the rules
are a one-to-one port of `scripts/llmd-router/lifecycle-sim/sim.py` `admission_filter`.

## Placement

After `session-health-gate` (if present), BEFORE `session-affinity-filter` (if present), before
all scorers. A filter can only shrink the list; once the session filter has narrowed a request
to `[draining engine]` nothing downstream can move it.

## Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `minAdvantageTokens` | int | `0` | A draining endpoint keeps an unbound request only when its prefix advantage is at least this many tokens. `0` = one prefix block (`PrefixCacheMatchInfo.BlockSizeTokens()`, 1 token if no candidate carries match info). |
| `burstMinutes` | number | `5` | Migrate token-bucket capacity = `budget-tpm` × `burstMinutes`. Must be > 0. |
| `sessionAffinityPluginName` | string | `""` | Declared name of the `session-affinity-filter` (strategy `session_id`) whose bindings are read. Empty = prefix-only plane. A missing name or a non-`session_id` instance fails config load. |
| `recentBindingWindowSeconds` | int | `600` | A binding refreshed within this window counts as warming load / "recent". Must be > 0. |

## Endpoint labels

| Label | Values | Used when |
|---|---|---|
| `impala.ai/state` | `warming`, `draining` (absent = active) | always |
| `impala.ai/phase` | `retain`, `migrate` | draining |
| `impala.ai/share` | decimal in `[0,1]`, at most four places (`^(0(\.[0-9]{1,4})?\|1(\.0{1,4})?)$`; `1` and `1.0` both full; `"0.25 "`, `+0.5`, `.5`, `1e0` and hex floats are malformed) | warming |
| `impala.ai/budget-tpm` | positive integer, tokens/min for THIS EPP replica | migrate |
| `impala.ai/box` | any string; same-box sibling preference on a move | optional |

Malformed or unknown values make the endpoint active and increment `impala_admission_label_errors_total` once per candidate evaluation; an explicit `impala.ai/state=active` is accepted silently. The counter has no endpoint label, so the offender is named once at Info (per distinct endpoint and error; forgotten when it parses cleanly again) in the EPP log.

## Rules (per request)

- draining/retain: keep for its bound session, or for an unbound request whose advantage
  (`match(D) − max match(others)`, tokens) ≥ `minAdvantageTokens`; otherwise drop it (`steered`).
- draining/migrate: what retain would keep is moved (`moved`, dropping D; same-box non-draining
  siblings preferred) when the bucket admits the advantage, else `deferred`. The bucket starts
  EMPTY, refills at `budget-tpm/60` tokens/s, and when FULL admits one move larger than its
  capacity (going negative). A budget-label change recreates it empty; leaving migrate drops it.
- warming W: for a placeable request (no binding, no candidate with advantage ≥ min) narrow to
  `[W]` with probability `share` (several warming endpoints split it), unless W's load ≥
  mean(load of the non-warming kept candidates) + 1. Load = recent bindings on session planes,
  in-flight requests on prefix planes (see below).
- Never empty; panics recovered (pass-through, counted). When the draining endpoint is the only candidate left (a pool of one, or the health gate removed the rest) the request stays on it, the migrate bucket is refunded and no `moved`/`steered` decision or moved tokens are recorded.

## Load signal

- **Session planes** (`sessionAffinityPluginName` set): the number of bindings to the endpoint
  refreshed within `recentBindingWindowSeconds`, from the session store's read-only
  `BindingLookup`. The counts come from the periodic refresh, not from a per-request scan, so
  the warming overload stop reads binding counts up to 15 s old. This is accepted: a warming
  engine can take a few extra sessions within one refresh window.
- **Prefix-only planes**: the EPP's own in-flight request count for the endpoint. The plugin
  reads it from upstream's exported `GetRequests` on the producer of type
  `inflight-load-producer`, one call per endpoint ID in the pool's `PodList`; the plugin
  declares the producer's attribute in `Consumes()` so `CreateMissingDataProducers` creates it
  when the profile has none. If the attribute is absent on a candidate, the scraped
  `RunningRequestsSize` is the fallback. The fork adds no method to the producer.

## Metrics (bare names, EPP :9090)

`impala_admission_decisions_total{endpoint,state,phase,decision}`,
`impala_admission_moved_tokens_total{endpoint}`, `impala_admission_bucket_tokens{endpoint}`,
`impala_admission_endpoint_load{endpoint}`, `impala_session_bindings{endpoint}`,
`impala_session_bindings_recent{endpoint}` (session planes), `impala_endpoint_errors_total{endpoint,code}`
(from the plugin's ResponseHeader hook, status 400-599), `impala_admission_label_errors_total`,
`impala_admission_panics_total`. `endpoint` = the endpoint's file-discovery `name`. The
per-endpoint gauges are published as soon as discovery lists endpoints, then every 15 s for
every endpoint in the pool; endpoints that leave the pool drop out at the next refresh. Full
catalog: `docs/metrics.md`, "Impala admission (fork)".

## Limitations

- **One instance per EPP process.** The metric collectors are package-global, so exactly one
  `impala-admission-filter` instance per EPP process is supported (a second instance would share
  and double-write the same series).
- **Partial trailing blocks.** The approximate prefix index hashes partial trailing blocks, so a
  prefix seen only by a draining engine gives it an advantage: new conversations sharing that
  prefix are retained there. This is normal when shared system prompts live on at least two
  engines (the advantage over the best other engine is then zero); it matters only for a prompt
  family that lives on a single engine.

## Example

```yaml
- type: utilization-filter
  name: session-health-gate
  parameters: {conditions: [{metric: waiting-queue, maxValue: 8}], fallbackOnEmpty: true}
- type: impala-admission-filter
  parameters:
    sessionAffinityPluginName: session-affinity-filter
    burstMinutes: 5
- type: session-affinity-filter
  parameters: {strategy: session_id, sessionIdConfig: {sources: [{header: x-session-id}], evictionTtlSeconds: 3600}}
```

## Fork provenance

Branch `impala/v0.10.0-lifecycle` = upstream `v0.10.0` + this package + the `BindingLookup`
accessor on the session_id store. The fork deliberately does NOT carry `v0.9.0-impala.1`'s
endpoint-health-filter commits (or its `:status` backport): the health filter's counters are per
plugin instance, not per endpoint, so they cannot serve a per-endpoint add gate; per-endpoint
errors come from this plugin's `impala_endpoint_errors_total` instead.
