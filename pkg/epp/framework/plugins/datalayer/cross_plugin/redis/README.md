# Redis Syncer

**Type:** `redis-syncer`
**Interface:** `CrossReplicaSyncer`

Shares endpoint and request coordination state between EPP replicas through
Redis. Each EPP publishes its local endpoint value. During `Set`, the plugin
prepares a peer aggregate in its in-process cache. `Get` reads the contributor's
live local value and combines it with the cached peer aggregate.

## Configuration

Enable this Alpha plugin with `--allow-experimental-plugins=true`.

```yaml
plugins:
  - type: redis-syncer
    name: redis
    parameters:
      address: my-router-redis:6379
      passwordFile: /var/run/secrets/redis/password
      stateTTL: 2s
      coordinationTTL: 180s

dataLayer:
  crossReplica:
    syncerPluginRef: redis
```

Parameters:

- `address`: Redis address. Defaults to `localhost:6379`.
- `passwordEnv`: Optional name of an environment variable containing the Redis
  password.
- `passwordFile`: Optional path to a file containing the Redis password.
  Trailing carriage returns and newlines are removed.
  `passwordEnv` and `passwordFile` are mutually exclusive. Plaintext
  `password` configuration is rejected.
- `db`: Redis database number. Defaults to `0`. Must be `0` when `cluster` is
  `true`.
- `cluster`: Set to `true` for a Redis Cluster, such as an ElastiCache
  replication group with cluster mode enabled. `address` is then one seed node
  or the configuration endpoint; the client discovers the other nodes and
  follows slot redirections. Each endpoint's state is published in its own
  transaction, one round trip per endpoint, because a cluster transaction
  cannot span hash slots. The endpoints of one 100-endpoint batch are published
  in sequence and share one `publishTimeout`. Defaults to `false`.
- `tls`: Present to connect over TLS 1.2 or newer, for example `tls: {}`. It
  applies to every node in cluster mode. Optional fields:
  - `caFile`: Path to a PEM bundle of CA certificates to trust instead of the
    system roots.
  - `serverName`: Name to verify in every server certificate instead of the
    host it was dialed by. Set it when the server certificate does not name the
    addresses the client dials, for example cluster nodes reached by IP.
- `keyPrefix`: Optional namespace prepended, followed by `:`, to every key the
  plugin writes. Use it to keep the keys of different EPP pools apart when they
  share one Redis, for example `llmd:poolA` and `llmd:poolB`. EPPs that must
  share state, such as the old and new deployments during a rollout, must use
  the same prefix. Leave out `{` and `}`: in cluster mode they make the prefix a
  hash tag, which puts every key on one shard. Defaults to no prefix.
- `stateTTL`: Expiration for each replica's endpoint state in Redis. Defaults to
  `2s` and must be at least `1ms`.
- `coordinationTTL`: Expiration for request-level coordination values. Defaults
  to `180s` and must be at least `1ms`.

### Publisher timing

Publisher settings are configured under `dataLayer.crossReplica`:

| Setting | Default | Purpose |
|---|---|---|
| `syncInterval` | `200ms` | Interval between endpoint publication ticks. |
| `publishTimeout` | `1s` | Context timeout for one endpoint publish (shared by its concurrent contributor writes), or one batch chunk per contributor when the syncer supports batching. Also bounds one endpoint deletion. Must be positive. |

The Redis syncer publishes endpoints in batches: the publisher calls the
syncer once per contributor per cycle with the live endpoint set, without
holding its endpoint lock, and each chunk runs under one `publishTimeout`. Syncers without batch
support visit endpoints sequentially, with concurrent contributor writes for
each endpoint. Endpoints removed while a batch runs are deleted again after
it finishes. A slow cycle can exceed `syncInterval`. Choose `stateTTL` longer
than the observed interval between successful refreshes of an endpoint, with
margin for Redis latency and variation in cycle duration.

Deletion waits for the publisher's write lock, then starts one timeout shared by
all contributor deletions. An earlier caller deadline takes precedence for both
publication and deletion.

The Redis client honors context deadlines. Cancellation cannot interrupt mutex
waits or local computation, so `publishTimeout` does not guarantee that the
entire operation returns within that duration. Scheduling-path `Get` calls do
not acquire the publisher mutex.

`GetOrSet` uses its caller's context to bound Redis access; `coordinationTTL`
controls how long the stored value is retained. Redis connection, read, and
write timeouts use the client library's defaults and have no plugin parameters.

The `v1alpha1` field names are listed in the
[configuration API migration guide](../../../../../../../docs/architecture.md#api-versions).

### Redis requirements

Redis ACL usernames are not supported. Optional password authentication uses
the default Redis user. Managed Redis services that require an ACL username
cannot use this plugin.

Endpoint publication (`Set`) uses field expiration (`HPEXPIRE`) and requires
Redis 7.4 or newer. `GetOrSet` uses `SET NX GET` and requires Redis 7.0 or
newer. On an older server, such as ElastiCache for Redis OSS 7.1, every
endpoint publication fails; `GetOrSet` and the value store work.

Values stored under the same state key must use a compatible `encoding/gob`
schema. Each EPP decodes peer values into the type supplied by its local
contributor without requiring global `gob.Register` calls.

## State Model

Every key below is preceded by `<keyPrefix>:` when `keyPrefix` is set.

Endpoint state is stored in one Redis hash per state key and endpoint. Each EPP
owns one field in that hash, identified by its hostname and a process-specific
UUID. `Set` refreshes the field TTL, reads the hash in the same transaction,
and caches the prepared peer aggregate locally until the next successful `Set`
replaces it. A replica that stops publishing loses its field `stateTTL` after
its last write, and the next refresh drops it from the aggregate. `Get` reads
the cache without accessing Redis and combines it with the contributor's live
local value. If no peer value is available, `Get` returns the local value.

Request-level coordination uses separate string keys and `SET NX GET` so the
first value stored for a request is selected atomically across EPP replicas.

## Value Store

The plugin also implements the optional `CrossReplicaValueStore` interface:
string values that every EPP replica reads and writes atomically, one key per
value, each with an expiration the caller chooses. Plugins that keep a small
amount of shared state per request key, such as a session-to-endpoint binding,
use it instead of publishing whole tables every `syncInterval`.

| Method | Redis command | Minimum Redis | Result |
|---|---|---|---|
| `LoadAndRefresh` | `GETEX key PX ttl` | 6.2 | The stored value, with its expiration reset to `ttl`. |
| `LoadOrStore` | `SET key value NX GET PX ttl` | 7.0 | The value stored afterwards: the caller's, or the one another replica stored first. |
| `CompareAndSwap` | Lua script (`EVALSHA`, then `EVAL`) | 7.0 | Stores the new value when the key holds `old` or nothing, and returns the value stored afterwards. |

Keys are `<keyPrefix>:kv:<stateKey>:<id>`. Each call touches one key, so in
cluster mode the keys spread across shards. `ttl` must be at least `1ms`.
Every call is bounded by its caller's context and returns Redis errors as is;
the caller decides how to fall back.

## Scheduling and Admission

Periodic endpoint synchronization provides peer-aware scheduling state. It is
eventually consistent: two EPP replicas can make scheduling or admission
decisions before either observes the other's latest work.

This synchronization does not implement an atomic cluster-wide admission
quota. Flow control admission across replicas, including fixed per-replica
shares, is described in
[Multiple active replicas](../../../../../flowcontrol/README.md#multiple-active-replicas).

`GetOrSet` coordinates one value for one request identifier. It does not reserve
capacity across different requests.

## Deployment

The plugin is a Redis client and does not deploy a Redis server. The server must
be provisioned separately. Plugin construction does not connect to Redis, so an
unavailable server does not prevent the EPP from starting. Failed publications
are logged at error level with rate limiting and leave the peer cache unchanged,
so scheduling keeps counting the last known peer state until a publication
succeeds. A peer that stops while Redis is unreachable keeps counting until
then. Before its first successful publication an EPP uses local state only.

`GetOrSet` does not use a local fallback. It returns Redis errors because its
callers require cross-replica atomicity.

## Related Documentation

- [Plugins Index](../../../README.md)
