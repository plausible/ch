# Compression

ClickHouse supports compressing HTTP response bodies and accepting compressed request bodies. `Ch` keeps compression explicit: callers choose when to ask for compressed data by passing HTTP headers.

## Response Compression

Ask ClickHouse to compress a response with `accept-encoding`:

```elixir
Ch.query!(
  pool,
  "SELECT number FROM system.numbers LIMIT 1_000_000",
  %{},
  headers: [{"accept-encoding", "zstd"}]
)
```

Supported automatic decompression:

| Response kind | `content-encoding: zstd` | `content-encoding: gzip` | Other encodings |
| --- | --- | --- | --- |
| decoded `RowBinaryWithNamesAndTypes` success | decompressed automatically | decompressed automatically | raises |
| error response | decompressed automatically | decompressed automatically | raises |
| raw successful response | stored as received in `Ch.Result.data` | stored as received in `Ch.Result.data` | stored as received in `Ch.Result.data` |

By default, `Ch.query/4` requests `RowBinaryWithNamesAndTypes`, decodes it, and returns `%Ch.Result{names: names, rows: rows, headers: headers, data: data}`. If you add `accept-encoding: zstd` or `accept-encoding: gzip`, `Ch` decompresses before decoding.

If you override the response format, `Ch` returns `%Ch.Result{}` with the successful body as received in `data`:

```elixir
%Ch.Result{data: csv_gz} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 1_000_000",
    %{},
    headers: [
      {"x-clickhouse-format", "CSV"},
      {"accept-encoding", "gzip"}
    ]
  )
```

In that example, `csv_gz` is still gzip-compressed. This is intentional so callers can write compressed exports directly.

## Request Compression

To send a compressed request body, compress the body and set `content-encoding`:

```elixir
names = ["id", "name"]
types = ["UInt8", "String"]
rows = [[1, "one"], [2, "two"]]

payload =
  :zstd.compress([
    "INSERT INTO users FORMAT RowBinaryWithNamesAndTypes\n",
    Ch.RowBinary.encode_names_and_types(names, types),
    Ch.RowBinary.encode_rows(rows, types)
  ])

Ch.query!(
  pool,
  payload,
  %{},
  headers: [{"content-encoding", "zstd"}]
)
```

ClickHouse decompresses the request body before parsing the SQL and input format.

The uncompressed form is the same explicit RowBinary insert body:

```elixir
Ch.query!(pool, [
  "INSERT INTO users FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types),
  Ch.RowBinary.encode_rows(rows, types)
])
```

## Why ZSTD Is Not Default

`Ch` does not add `accept-encoding: zstd` automatically.

Compression is useful for large responses, but making it the default would also affect small queries and raw export workflows. Keeping it explicit means:

- small queries avoid compression overhead;
- raw responses can be stored exactly as ClickHouse sent them;
- compressed CSV/RowBinary exports can be written directly;
- callers choose the tradeoff per query.

For large decoded query results, prefer:

```elixir
headers: [{"accept-encoding", "zstd"}]
```

## Errors

Error bodies are always treated as part of Ch's API, not as raw payloads. If ClickHouse returns an error with `content-encoding: zstd` or `content-encoding: gzip`, `Ch` decompresses it before building `%Ch.Error{}`.
