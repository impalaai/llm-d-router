# Session Affinity Scorer

**Type:** `session-affinity-scorer`

Scores candidate pods by giving a higher score to the pod that was previously used for the same session, and zero to the rest. Enables sticky routing for stateful workloads where reusing the same pod reduces latency or preserves context.

Supports two algorithms, selected by the `strategy` parameter:

- `encoded_endpoint_header` (default): stateless. The session is carried in a request header whose value is the base64-encoded `namespace/name` of the previously selected pod. As a [`ResponseHeaderProcessor`](../../../../interface/requesthandling/plugins.go), the scorer writes that same header on the response so the client can echo it back on the next request.
- `session_id`: stateful. The client supplies an opaque session identifier, read from one or more configured sources (a request header, or a request attribute published by an upstream plugin) tried in priority order. The scorer maintains a server-side, TTL-evicted binding from that identifier to the pod that served it. Nothing is written back to the client. An unbound session expresses no preference: the scorer gives every candidate zero and lets the picker place the request. A bound session whose pod is absent from the candidate set also scores zero everywhere, then rebinds to the pod the request was routed to.

## Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `strategy` | string | `encoded_endpoint_header` | `encoded_endpoint_header` or `session_id`. Only the config block matching the strategy is used. |
| `profileName` | string | | The name of the profile this instance is associated with. When set (e.g. `prefill`), the plugin looks up the target pod from the results of that profile in `SchedulingResult`. It must name a profile in `schedulingProfiles`, or loading the config fails. When empty, it defaults to the primary (decode) pod. |
| `encodedEndpointHeaderConfig` | object | | Config for `encoded_endpoint_header`; see below. |
| `sessionIdConfig` | object | | Config for `session_id`; see below. |

`encodedEndpointHeaderConfig`:

| Name | Type | Default | Description |
|---|---|---|---|
| `header` | string | `x-session-token` | Request and response header carrying the base64-encoded pod token. |

`sessionIdConfig`:

| Name | Type | Default | Description |
|---|---|---|---|
| `sources` | list | one `header` source `x-session-id` | Where the session identifier is read from, in priority order; the first non-empty match wins. Each source sets exactly one of `header` (a request header name) or `attribute` (a request-attribute key published by an upstream plugin). |
| `evictionTtlSeconds` | float | `300` | How long a session binding survives unused. |
| `evictionSweepSeconds` | float | `10` | How often expired bindings are swept. |
| `syncCrossReplicaState` | bool | `true` | Share session bindings with other EPP replicas when the configured cross-replica syncer stores per-key values; see [Multiple EPP replicas](#multiple-epp-replicas). |
| `storeTimeoutSeconds` | float | `0.05` | Time limit for the shared-store calls of one request; see [Multiple EPP replicas](#multiple-epp-replicas). |

### Default Configuration (without PD disaggregation)

```yaml
- type: session-affinity-scorer
```

### Session ID Configuration

```yaml
- type: session-affinity-scorer
  parameters:
    strategy: session_id
    sessionIdConfig:
      sources:
        - header: x-session-id
      evictionTtlSeconds: 300
      evictionSweepSeconds: 10
```

To pin agent traffic, list the agent-identity attribute as a fallback source after the header:

```yaml
- type: session-affinity-scorer
  parameters:
    strategy: session_id
    sessionIdConfig:
      sources:
        - header: x-session-id
        - attribute: agent-identity
```

### PD Disaggregation Configuration

To support session affinity with PD disaggregation, configure two separate instances of the scorer: one for decode and one for prefill.

```yaml
# Instance for the decode profile (pins decode requests)
- name: session-affinity-decode
  type: session-affinity-scorer
  parameters:
    encodedEndpointHeaderConfig:
      header: x-session-token

# Instance for the prefill profile (pins prefill requests)
- name: session-affinity-prefill
  type: session-affinity-scorer
  parameters:
    profileName: prefill
    encodedEndpointHeaderConfig:
      header: x-session-token-prefill
```

The decode instance uses the default behavior (writing the decode pod to `x-session-token`). The prefill instance uses `profileName: prefill` to look up the prefill pod from the scheduling results and write it to `x-session-token-prefill`. This ensures that subsequent requests in the same session target both the same prefill pod and the same decode pod. `session_id` supports the same pattern: configure one instance per profile, each with its own `profileName`.

## Multiple EPP replicas

`session_id` bindings are shared between EPP replicas when `syncCrossReplicaState` is `true` and the configured cross-replica syncer (`dataLayer.crossReplica`) stores per-key values. `redis-syncer` does; see its [Value Store](../../../datalayer/cross_plugin/redis/README.md#value-store). With any other syncer, or none, bindings stay local to each replica.

Each binding is one key in the store, named by the 64-bit xxhash of the session identifier and holding the endpoint ID. Session identifiers themselves are not stored. The key expires `evictionTtlSeconds` after its last use.

For each request that carries a session identifier:

1. The plugin reads the key, which also resets its expiration. A stored binding replaces the replica's local one.
2. A session with no stored binding is scheduled normally. After the request is routed, the replica stores the binding unless another replica stored one first, and then uses the stored one.
3. A stored binding whose pod is not among the request's candidates is replaced with the routed pod, unless another replica replaced it first.

A known session costs one store call per request and a new session two. A replica that starts with an empty local map finds existing bindings in the store.

The request that creates or replaces a binding is routed before the store answers. When several replicas do this for one session at the same moment, each of those requests goes to the pod its own replica picked, and later requests all go to the stored pod.

When a store call fails or takes longer than `storeTimeoutSeconds`, the replica routes on its local binding, or schedules normally when it has none. A local binding is stored again on the next request the store answers and holds no binding for.

## Relationship to the session affinity filter

The [session affinity filter](../../filter/sessionaffinity/README.md) (`session-affinity-filter`) provides the same affinity behavior as a hard constraint. Configuring both alongside the scorer is unnecessary and can be misleading; see [Relationship to the session affinity scorer](../../filter/sessionaffinity/README.md#relationship-to-the-session-affinity-scorer) for details. Use the scorer for a soft preference that can be outweighed by other scorers, or the filter for a hard pin.
