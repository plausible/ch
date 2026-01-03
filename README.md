# Ch

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ch)

Minimal HTTP [ClickHouse](https://clickhouse.com) client for Elixir.

Used in [Ecto ClickHouse adapter.](https://github.com/plausible/ecto_ch)

### Key features

- RowBinary
- Native query parameters
- Per query settings
- Minimal API

Your ideas are welcome [here.](https://github.com/plausible/ch/issues/82)

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.6.0"}
  ]
end
```

## Usage

#### Start [DBConnection](https://github.com/elixir-ecto/db_connection) pool

```elixir
defaults = [
  scheme: "http",
  hostname: "localhost",
  port: 8123,
  database: "default",
  settings: [],
  pool_size: 1,
  timeout: :timer.seconds(15)
]

# note that starting in ClickHouse 25.1.3.23 `default` user doesn't have
# network access by default in the official Docker images
# see https://github.com/ClickHouse/ClickHouse/pull/75259
{:ok, pid} = Ch.start_link(defaults)
```

#### Select rows

```elixir
{:ok, pid} = Ch.start_link()

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT 3")

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT {$0:UInt8}", [3])

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 3})
```

Note on datetime encoding in query parameters:

- `%NaiveDateTime{}` is encoded as text to make it assume the column's or ClickHouse server's timezone
- `%DateTime{}` is encoded as unix timestamp and is treated as UTC timestamp by ClickHouse

#### Select rows (lots of params, reverse proxy)

> [!NOTE]
>
> Support for multipart requests was added in `v0.6.2`

For queries with many parameters the resulting URL can become too long for some reverse proxies, resulting in a `414 Request-URI Too Large` error.

To avoid this, you can use the `multipart: true` option to send the query and parameters in the request body.

```elixir
{:ok, pid} = Ch.start_link()

# Moves parameters from the URL to a multipart/form-data body
%Ch.Result{rows: [[[1, 2, 3 | _rest]]]} =
  Ch.query!(pid, "SELECT {ids:Array(UInt64)}", %{"ids" => Enum.to_list(1..10_000)}, multipart: true)
```

> [!NOTE]
>
> `multipart: true` is currently required on each individual query. Support for pool-wide configuration is planned for a future release.

#### Insert rows

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES (0), (1)")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({$0:UInt8}), ({$1:UInt32})", [0, 1])

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({a:UInt16}), ({b:UInt64})", %{"a" => 0, "b" => 1})

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) SELECT number FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 2})
```

#### Insert rows as [RowBinary](https://clickhouse.com/docs/en/interfaces/formats/RowBinary) (efficient)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

types = ["UInt64"]
# or
types = [Ch.Types.u64()]
# or
types = [:u64]

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT RowBinary", [[0], [1]], types: types)
```

Note that RowBinary format encoding requires `:types` option to be provided.

Similarly, you can use [RowBinaryWithNamesAndTypes](https://clickhouse.com/docs/en/interfaces/formats/RowBinaryWithNamesAndTypes) which would additionally do something like a type check.

```elixir
sql = "INSERT INTO ch_demo FORMAT RowBinaryWithNamesAndTypes"
opts = [names: ["id"], types: ["UInt64"]]
rows = [[0], [1]]

%Ch.Result{num_rows: 2} = Ch.query!(pid, sql, rows, opts)
```

#### Insert rows in custom [format](https://clickhouse.com/docs/en/interfaces/formats)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

csv = [0, 1] |> Enum.map(&to_string/1) |> Enum.intersperse(?\n)

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT CSV", csv, encode: false)
```

#### Insert rows as chunked RowBinary stream

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

stream = Stream.repeatedly(fn -> [:rand.uniform(100)] end)
chunked = Stream.chunk_every(stream, 100)
encoded = Stream.map(chunked, fn chunk -> Ch.RowBinary.encode_rows(chunk, _types = ["UInt64"]) end)
ten_encoded_chunks = Stream.take(encoded, 10)

%Ch.Result{num_rows: 1000} =
  Ch.query(pid, "INSERT INTO ch_demo(id) FORMAT RowBinary", ten_encoded_chunks, encode: false)
```

This query makes a [`transfer-encoding: chunked`](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) HTTP request while unfolding the stream resulting in lower memory usage.

#### Query with custom [settings](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
{:ok, pid} = Ch.start_link()

settings = [async_insert: 1]

%Ch.Result{rows: [["async_insert", "Bool", "0"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'")

%Ch.Result{rows: [["async_insert", "Bool", "1"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'", [], settings: settings)
```

## Caveats

#### NULL in RowBinary

It's the same as in [ch-go](https://clickhouse.com/docs/en/integrations/go#nullable)

> At insert time, Nil can be passed for both the normal and Nullable version of a column. For the former, the default value for the type will be persisted, e.g., an empty string for string. For the nullable version, a NULL value will be stored in ClickHouse.

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, """
CREATE TABLE ch_nulls (
  a UInt8 NULL,
  b UInt8 DEFAULT 10,
  c UInt8 NOT NULL
) ENGINE Memory
""")

types = ["Nullable(UInt8)", "UInt8", "UInt8"]
inserted_rows = [[nil, nil, nil]]
selected_rows = [[nil, 0, 0]]

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, "INSERT INTO ch_nulls(a, b, c) FORMAT RowBinary", inserted_rows, types: types)

%Ch.Result{rows: ^selected_rows} =
  Ch.query!(pid, "SELECT * FROM ch_nulls")
```

Note that in this example `DEFAULT 10` is ignored and `0` (the default value for `UInt8`) is persisted instead.

However, [`input()`](https://clickhouse.com/docs/en/sql-reference/table-functions/input) can be used as a workaround:

```elixir
sql = """
INSERT INTO ch_nulls
  SELECT * FROM input('a Nullable(UInt8), b Nullable(UInt8), c UInt8')
  FORMAT RowBinary\
"""

Ch.query!(pid, sql, inserted_rows, types: ["Nullable(UInt8)", "Nullable(UInt8)", "UInt8"])

%Ch.Result{rows: [[0], [10]]} =
  Ch.query!(pid, "SELECT b FROM ch_nulls ORDER BY b")
```

#### UTF-8 in RowBinary

When decoding [`String`](https://clickhouse.com/docs/en/sql-reference/data-types/string) columns non UTF-8 characters are replaced with `�` (U+FFFD). This behaviour is similar to [`toValidUTF8`](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#tovalidutf8) and [JSON format.](https://clickhouse.com/docs/en/interfaces/formats#json)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE ch_utf8(str String) ENGINE Memory")

bin = "\x61\xF0\x80\x80\x80b"
utf8 = "a�b"

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, "INSERT INTO ch_utf8(str) FORMAT RowBinary", [[bin]], types: ["String"])

%Ch.Result{rows: [[^utf8]]} =
  Ch.query!(pid, "SELECT * FROM ch_utf8")

%Ch.Result{rows: %{"data" => [[^utf8]]}} =
  pid |> Ch.query!("SELECT * FROM ch_utf8 FORMAT JSONCompact") |> Map.update!(:rows, &Jason.decode!/1)
```

To get raw binary from `String` columns use `:binary` type that skips UTF-8 checks.

```elixir
%Ch.Result{rows: [[^bin]]} =
  Ch.query!(pid, "SELECT * FROM ch_utf8", [], types: [:binary])
```

#### Timezones in RowBinary

Decoding non-UTC datetimes like `DateTime('Asia/Taipei')` requires a [timezone database.](https://hexdocs.pm/elixir/DateTime.html#module-time-zone-database)

```elixir
Mix.install([:ch, :tz])

:ok = Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

{:ok, pid} = Ch.start_link()

%Ch.Result{rows: [[~N[2023-04-25 17:45:09]]]} =
  Ch.query!(pid, "SELECT CAST(now() as DateTime)")

%Ch.Result{rows: [[~U[2023-04-25 17:45:11Z]]]} =
  Ch.query!(pid, "SELECT CAST(now() as DateTime('UTC'))")

%Ch.Result{rows: [[%DateTime{time_zone: "Asia/Taipei"} = taipei]]} =
  Ch.query!(pid, "SELECT CAST(now() as DateTime('Asia/Taipei'))")

"2023-04-26 01:45:12+08:00 CST Asia/Taipei" = to_string(taipei)
```

Encoding non-UTC datetimes works but might be slow due to timezone conversion:

```elixir
Mix.install([:ch, :tz])

:ok = Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE ch_datetimes(name String, datetime DateTime) ENGINE Memory")

naive = NaiveDateTime.utc_now()
utc = DateTime.utc_now()
taipei = DateTime.shift_zone!(utc, "Asia/Taipei")

rows = [["naive", naive], ["utc", utc], ["taipei", taipei]]

Ch.query!(pid, "INSERT INTO ch_datetimes(name, datetime) FORMAT RowBinary", rows, types: ["String", "DateTime"])

%Ch.Result{
  rows: [
    ["naive", ~U[2024-12-21 05:24:40Z]],
    ["utc", ~U[2024-12-21 05:24:40Z]],
    ["taipei", ~U[2024-12-21 05:24:40Z]]
  ]
} =
  Ch.query!(pid, "SELECT name, CAST(datetime as DateTime('UTC')) FROM ch_datetimes")
```

## [Benchmarks](./bench)

See nightly [CI runs](https://github.com/plausible/ch/actions/workflows/bench.yml) for latest results.
