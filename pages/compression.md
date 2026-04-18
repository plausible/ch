# Compression

ClickHouse HTTP accepts compressed request bodies via the `content-encoding` header.
Compress the **entire request body** — including the SQL statement prefix for INSERTs —
before sending. `Ch.HTTP.decode/3` automatically decompresses `gzip` and `zstd` responses.

**Further reading:**
- [Compression in ClickHouse](https://clickhouse.com/docs/data-compression/compression-in-clickhouse)
- [Compression modes](https://clickhouse.com/docs/data-compression/compression-modes)
- [Optimizing with schemas and codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- [Input format matchup: which is fastest](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient) — [FastFormats benchmark](https://fastformats.clickhouse.com)
- [Supercharging large data loads](https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part2)
- [What really matters for performance](https://clickhouse.com/blog/what-really-matters-for-performance-lessons-from-a-year-of-benchmarks)

## zstd (preferred, stdlib)

`:zstd` is part of OTP 28 stdlib. ZSTD is also ClickHouse Cloud's default column
compression codec. For HTTP transport, ZSTD achieves 30–50% smaller payloads than LZ4 at
the cost of more CPU on the client during compression.
**Use ZSTD when bandwidth costs money** (cross-region, CDN egress) or when your client
has idle CPU — the server decompresses quickly enough that the transfer saving usually wins.

```elixir
rows = Ch.RowBinary.encode_rows([[1, "pageview", DateTime.utc_now()]], types)
body = IO.iodata_to_binary([
  "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types),
  rows
])

compressed = :zstd.compress(body)

{path, headers, body} =
  Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "zstd"}])

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, headers, body)
```

> The SQL statement and RowBinary payload must be compressed together as one blob.
> Only compressing the data rows and leaving the statement uncompressed does not work.

## gzip (stdlib, fallback)

`:zlib.gzip/1` is available on all OTP versions. Lower compression ratio and slower than
ZSTD, but zero extra dependencies. Good choice if OTP < 28 or you need maximum compatibility.

```elixir
compressed = :zlib.gzip(body)

{path, headers, body} =
  Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "gzip"}])
```

`Ch.HTTP.decode/3` decompresses `gzip` responses automatically.

## lz4 (nimble_lz4)

LZ4 compresses and decompresses faster than both gzip and zstd, with a moderate ratio.
Per the [FastFormats benchmark](https://fastformats.clickhouse.com), LZ4 is a
"no-brainer" for same-region deployments: it cuts wire size roughly in half with
negligible CPU overhead on both client and server.

```elixir
{:ok, compressed} = NimbleLz4.compress(body)

{path, headers, body} =
  Ch.HTTP.encode(compressed, %{}, headers: [{"content-encoding", "lz4"}])
```

Add to deps:

```elixir
{:nimble_lz4, "~> 1.1"}
```

## Response decompression

`Ch.HTTP.decode/3` decompresses responses with `content-encoding: gzip` or
`content-encoding: zstd` automatically. For other encodings (e.g. `lz4`),
decompress the body before calling `decode/3`, or simply do not request compressed
responses (the default — ClickHouse sends uncompressed unless you add
`Accept-Encoding` to the request).

## Which to use?

| | gzip | zstd | lz4 |
|---|---|---|---|
| Dep | none (stdlib) | none (OTP ≥ 28) | `nimble_lz4` |
| Ratio | good | best | moderate |
| Client CPU | medium | high | low |
| Server CPU | medium | low | lowest |
| Best for | compatibility | bandwidth-sensitive | same-region throughput |

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

See [`test/ch/guides/compression_test.exs`](../test/ch/guides/compression_test.exs).
