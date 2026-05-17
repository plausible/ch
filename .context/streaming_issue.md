## Context

The current rewrite is moving toward a small eager API first:

- `Ch.request/4` as a raw transport-style request returning response headers and body iodata.
- `Ch.query/4` as the ergonomic decoded path, likely built on top of `request/4`.
- Successful raw responses should stay raw, including compressed bytes when the caller requested `accept-encoding`.
- Errors should still be collected/decompressed internally so `%Ch.Error{}` has a useful message.

This leaves streaming as a separate API design question instead of mixing it into the first eager implementation.

## Streaming ideas

Possible follow-up APIs:

```elixir
Ch.request(pool, sql, params, into: collectable)
Ch.request(pool, sql, params, into: fun)
Ch.stream(pool, sql, params, opts)
```

`into: collectable` would support direct raw exports, for example compressed CSV/RowBinary to a file, without accumulating the response body in memory.

`into: fun` could mirror Finch-style callback streaming with events such as:

```elixir
{:status, status}
{:headers, headers}
{:data, chunk}
```

A later `Ch.stream/4` could implement `Enumerable`, but it needs careful design with `NimblePool`: the connection must stay checked out for the lifetime of enumeration, so a plain `Stream.resource/3` wrapper is probably the wrong shape unless the whole reduce happens inside the checkout callback.

## Error handling rule to preserve

For any streaming/collectable mode:

- On 2xx, stream/write chunks according to caller intent.
- On non-2xx, do not write chunks to the caller's collectable/callback.
- Collect the error body internally, decompress it if `content-encoding` is `gzip` or `zstd`, and return `{:error, %Ch.Error{}}`.
- If the caller halts early, close/remove the connection unless we deliberately drain the response.

## Prior art

Finch has `request/3`, `stream/5`, and `stream_while/5`.
Req builds on Finch with `into:` supporting callback, collectable, and `:self` modes.

For Ch, starting with eager `request/query` and documenting streaming as future work keeps the rewrite smaller while preserving a clean path for raw exports later.
