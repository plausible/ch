# Compression

ClickHouse HTTP accepts compressed request bodies via the `content-encoding` header.
Compress the **entire request body** — including the SQL statement prefix for INSERTs —
before sending.

**Further reading:**
- [Compression in ClickHouse](https://clickhouse.com/docs/data-compression/compression-in-clickhouse)
- [Compression modes](https://clickhouse.com/docs/data-compression/compression-modes)
- [Optimizing with schemas and codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- [Input format matchup: which is fastest](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient) — [FastFormats benchmark](https://fastformats.clickhouse.com)
- [Supercharging large data loads](https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part2)
- [What really matters for performance](https://clickhouse.com/blog/what-really-matters-for-performance-lessons-from-a-year-of-benchmarks)

## ZSTD

`:zstd` is part of OTP 28 stdlib.

**Use ZSTD when bandwidth costs money** (cross-region, CDN egress) or when your client
has idle CPU — the server decompresses quickly enough that the transfer saving usually wins.

```elixir
statement = "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n"

names = ["id", "name", "created_at"]
types = ["UInt64", "String", "DateTime"]

rows = [
  [1, "pageview", DateTime.utc_now()],
  [2, "click", DateTime.utc_now()],
  [3, "purchase", DateTime.utc_now()]
]

row_binary_with_names_and_types = [
  Ch.RowBinary.encode_names_and_types(names, types)
  | Ch.RowBinary.encode_rows(rows, types)
]

body = :zstd.compress([statement | row_binary_with_names_and_types])

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", "/", [{"content-encoding", "zstd"}], body)
```

## LZ4

LZ4 is not part of OTP stdlib but you can use [NimbleLZ4](https://github.com/whatyouhide/nimble_lz4).

Per the [FastFormats benchmark](https://fastformats.clickhouse.com), LZ4 is a
"no-brainer" for same-region deployments: it cuts wire size roughly in half with
negligible CPU overhead on both client and server.

However, due to NimbleLZ4 using dirty CPU schedulers for the NIF calls, the compression
speed is similar to `:zstd` for small payloads, so compare on your own data before making a decision.

```elixir
statement = "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n"

names = ["id", "name", "created_at"]
types = ["UInt64", "String", "DateTime"]

rows = [
  [1, "pageview", DateTime.utc_now()],
  [2, "click", DateTime.utc_now()],
  [3, "purchase", DateTime.utc_now()]
]

row_binary_with_names_and_types = [
  Ch.RowBinary.encode_names_and_types(names, types)
  | Ch.RowBinary.encode_rows(rows, types)
]

body = NimbleLZ4.compress([statement | row_binary_with_names_and_types])

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", "/", [{"content-encoding", "lz4"}], body)
```

## Response decompression

`Ch.HTTP` does not decompress responses. Decompress the body yourself
before decoding.

To receive a compressed response, add `accept-encoding` header to your request.
ClickHouse sends uncompressed responses by default.

```elixir
headers = [
  {"accept-encoding", "zstd"},
  {"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}
]

statement = "SELECT * FROM events WHERE name = {name:String}"

params = %{"name" => "pageview"}
options = %{"query_id" => "123"}
path = Ch.HTTP.path(params, options)

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, headers, statement)

deadline = Ch.HTTP.to_deadline(to_timeout(second: 5))

case Mint.HTTP1.recv(conn, 0, Ch.HTTP.to_timeout(deadline)) do
  {:ok, _ref, responses} ->
    case handle_responses(responses) do
      {:ok, _ref, conn} ->
        {:ok, _ref, conn}

      {:error, reason} ->
        {:error, reason}
    end

  {:error, reason} ->
    {:error, reason}
end

# 1. Accumulate Mint responses (manual recv loop omitted for brevity)
# {:ok, conn, responses} = Mint.HTTP1.recv(conn, 0, 5_000)

# 2. Extract and decompress
resp_body = Enum.find_value(responses, fn {:data, _ref, data} -> data end)
resp_headers = Enum.find_value(responses, fn {:headers, _ref, h} -> h end)

body =
  case List.keyfind(resp_headers, "content-encoding", 0) do
    {_, "zstd"} -> :zstd.decompress(resp_body)
    {_, "gzip"} -> :zlib.gunzip(resp_body)
    _ -> resp_body
  end

# 3. Decode
state = Ch.HTTP.decode_start()
# ... Feed responses (status, headers, body, done) to decode_continue ...
# (See README or Ch.HTTP for the full loop)
```

For INSERT and DDL, responses are always empty — no decompression needed.

## Which to use?

ClickHouse's own benchmark ([FastFormats](https://fastformats.clickhouse.com)) shows
RowBinaryWithNamesAndTypes+LZ4 reduces payload to ~60% of uncompressed size with
minimal overhead. ZSTD takes it to ~30% of original. In same-region tests, those
extra CPU cycles slightly offset the bandwidth saving; in cross-region or metered
bandwidth scenarios, ZSTD wins overall.

## Column-level compression (on-disk, not HTTP)

The compression you choose for HTTP transport is separate from ClickHouse's on-disk
column codec. ClickHouse Cloud defaults to `ZSTD(1)` for column storage.
Tune per-column codecs in your `CREATE TABLE` DDL:

```sql
CREATE TABLE events (
  id     UInt64,
  name   LowCardinality(String),
  ts     DateTime64(3, 'UTC') CODEC(Delta, ZSTD)
) ENGINE = MergeTree ORDER BY (name, ts)
```

See [Optimizing with schemas and codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
for guidance on `Delta`, `DoubleDelta`, `Gorilla`, `T64`, and when each helps.

## Tests

See [`test/ch/guides/compression_test.exs`](../test/ch/guides/compression_test.exs) for more examples.
