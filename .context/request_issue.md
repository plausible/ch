## Context

The current rewrite keeps only `Ch.query/4` for now.

We discussed adding a lower-level `Ch.request/4` that would return response headers and raw body iodata without decoding successful responses. That would be useful, but it is not needed to land the current rewrite and would expand the public API before the basic query path has settled.

## Possible future API

```elixir
Ch.request(pool, sql, params, opts)
# => {:ok, headers, body_iodata} | {:error, %Ch.Error{}}
```

Possible semantics:

- Raw transport-style API.
- Successful responses are returned as received.
- No automatic success decompression.
- No automatic success RowBinary decoding.
- Non-2xx responses are still collected and converted into `%Ch.Error{}`.
- Compressed error bodies are decompressed internally so `%Ch.Error.message` remains useful.
- Request options would likely match `Ch.query/4`: `:headers`, `:settings`, `:timeout`.

`Ch.query/4` could later be implemented on top of this primitive:

```elixir
with {:ok, headers, body} <- Ch.request(pool, sql, params, opts) do
  decode_query_response(headers, body)
end
```

## Why defer it

- `Ch.query/4` is the only API needed for the current rewrite.
- Adding `request/4` now forces decisions about raw response shape, decompression, status exposure, and future streaming before there is enough pressure from real usage.
- Keeping one public function reduces churn while the NimblePool/HTTP rewrite stabilizes.
- Streaming/raw export APIs can still be added later without breaking `Ch.query/4`.

## Related

Streaming/raw export ideas are tracked separately in #342.
