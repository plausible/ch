# Multi-host

> **Planned, not yet implemented.**

Support for multiple ClickHouse endpoints — load balancing, read replicas, failover —
is planned for `Ch.Pool`. This page describes the intended design.

## Comparison: clickhouse-go

The [Go client](https://clickhouse.com/docs/integrations/language-clients/go/configuration#connecting-to-multiple-nodes)
is the closest reference point given it also targets the HTTP interface.

```go
conn, err := clickhouse.Open(&clickhouse.Options{
  Addr: []string{"ch-1:8123", "ch-2:8123", "ch-3:8123"},
  ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
  // ...
})
```

Go exposes three strategies via `ConnOpenStrategy`:

| Strategy | Behaviour |
|---|---|
| `ConnOpenInOrder` (default) | Try first address; fall back to later ones only on failure. Pure failover / primary-replica. |
| `ConnOpenRoundRobin` | Cycle through addresses in order. |
| `ConnOpenRandom` | Pick at random. |

Notable points from the Go docs:
- Pool sizing is `MaxOpenConns` / `MaxIdleConns` (default 5/10).
- `ConnMaxLifetime` defaults to **1 hour**. Their own docs warn this causes load imbalance
  when a failed node recovers — connections won't rebalance for up to an hour. They recommend
  lowering it for heavy workloads.
- No down-host tracking. The `InOrder` strategy retries later addresses, but there's no
  internal "mark this host bad" state.

**Where ch already does better**: `worker_idle_timeout` defaults to 5 seconds. A worker
that has been idle for 5 s is removed, which means connections naturally cycle far more
aggressively than Go's 1-hour lifetime. Ch workers reconnect to a (potentially new) host
on the next checkout. No special `ConnMaxLifetime` concept needed. That said, if all
pool workers are always busy (high load), idle timeout never fires — the connection TTL
question from the previous section still applies.

**Where Go does better**: Three configurable strategies including `InOrder`
(primary/replica failover) which ch does not plan to support initially.

## Proposed API

```elixir
Ch.Pool.start_link(
  urls: [
    "http://ch-1.internal:8123",
    "http://ch-2.internal:8123",
    "http://ch-3.internal:8123"
  ],
  pool_size: 15,
  worker_idle_timeout: to_timeout(second: 5),
  # optional, default: :random
  connect_strategy: :random  # | :round_robin | :in_order
)
```

`urls:` replaces the current `url:` option. A single-element list or the original `url:`
scalar are both accepted for backward compatibility.

## Connection strategies

### `:random` (default)

Pick uniformly at random from available (non-down) hosts. No shared counter, no
accumulated skew after worker churn. The correct default for symmetric replica sets.

### `:round_robin`

Increment a counter in pool state, take `rem(counter, length(hosts))`. Offers more
even initial distribution than random for small pools, but skews after disconnects just
as in Go. Suitable when you want deterministic spread and your pool is stable.

### `:in_order`

Try hosts in list order; only use later hosts if earlier ones are down.
Primary/replica failover: all traffic goes to `ch-1`, only spills to `ch-2` when
`ch-1` is unreachable. This is Go's `ConnOpenInOrder`.

Host selection happens in `handle_checkout/4` which has access to full pool state
including the down-host map, so all three strategies can filter out known-down hosts
before selection.

## Down-host tracking

The Go client has no equivalent of this — it's a ch addition.

When `ensure_connected` returns `{:error, reason}`, the caller does
`{:remove, {:connect_error, host, port, reason}}`. `handle_checkin` catches this
and marks the host down in pool state:

```elixir
%{
  hosts: [{:http, "ch-1.internal", 8123}, ...],
  down: %{
    {"ch-2.internal", 8123} => down_until_monotonic_ms
  }
}
```

**Connect error vs request error distinction**: if a connection was alive and the
request failed (timeout, CH error response), the host is fine — don't penalise it.
Only TCP-level connect failures mark a host down.

**Fallback when all hosts are down**: pick randomly from all hosts anyway. Better to
attempt a potentially-recovered connection and get a fast TCP refusal than to stall
the caller indefinitely.

## Re-enabling down hosts

No active probe process is needed. Down entries have a `down_until` timestamp.
`handle_checkout` ignores any host where `now < down_until`. Expired entries are
naturally bypassed and a periodic `handle_info` GC removes stale map entries:

```elixir
def init_pool(config) do
  schedule_gc()
  {:ok, config}
end

def handle_info(:gc_down_hosts, state) do
  now = System.monotonic_time(:millisecond)
  down = Map.reject(state.down, fn {_host, until} -> until <= now end)
  schedule_gc()
  {:ok, %{state | down: down}}
end
```

Default `down_ttl: to_timeout(second: 30)`, configurable.

## Connection TTL for always-busy pools

`worker_idle_timeout` (default 5 s) handles the quiet case: idle workers are removed
and replaced with fresh connections on next checkout. This is already much better than
Go's 1-hour default.

For high-load scenarios where all workers stay checked out continuously, idle timeout
never fires. A future `max_connection_age` option would forcibly recycle a connection
after a wall-clock TTL regardless of activity, checked on checkin:

```elixir
defp checkin(conn, connected_at, max_age) do
  if Mint.HTTP1.open?(conn) and max_age_ok?(connected_at, max_age) do
    {:ok, conn}
  else
    {:remove, :ttl_expired}
  end
end
```

Not planned for the initial implementation.

## Open questions

- Should `connect_strategy` default to `:random` or `:round_robin`? Random is simpler
  and avoids skew; round-robin is more intuitive.
- Should `urls:` accept `host:port` pairs (like Go) in addition to full URLs? Full URLs
  are more explicit about scheme and path; `host:port` is less typing.
- Telemetry event when all hosts are down and the fallback kicks in?
- `down_ttl` configurable per-pool or fixed?
