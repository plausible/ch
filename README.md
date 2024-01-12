# Ch

[![Hex Package](https://img.shields.io/hexpm/v/ch.svg)](https://hex.pm/packages/ch)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ch)

Minimal HTTP ClickHouse client for Elixir.

Used in [Ecto ClickHouse adapter.](https://github.com/plausible/chto)

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
    {:ch, "~> 0.2.0"}
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
- `%DateTime{time_zone: "Etc/UTC"}` is encoded as unix timestamp and is treated as UTC timestamp by ClickHouse
- encoding non UTC `%DateTime{}` raises `ArgumentError`

#### Insert rows

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES (0), (1)")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({a:UInt16}), ({b:UInt64})", %{"a" => 0, "b" => 1})

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) SELECT number FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 2})
```

#### Insert [RowBinary](https://clickhouse.com/docs/en/interfaces/formats#rowbinary)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

types = ["UInt64"]
# or
types = [Ch.Types.u64()]
# or
types = [:u64]

rows = [[0], [1]]
row_binary = Ch.RowBinary.encode_rows(rows, types)

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, ["INSERT INTO ch_demo(id) FORMAT RowBinary\n" | row_binary])
```

Note that RowBinary format encoding requires `:types` option to be provided.

Similarly, you can use [`RowBinaryWithNamesAndTypes`](https://clickhouse.com/docs/en/interfaces/formats#rowbinarywithnamesandtypes) which would additionally do something like a type check.

```elixir
names = ["id"]
types = ["UInt64"]

header = Ch.RowBinary.encode_names_and_types(names, types)
row_binary = Ch.RowBinary.encode_rows(rows, types)

%Ch.Result{num_rows: 3} =
  Ch.query!(pid, [
    "INSERT INTO ch_demo FORMAT RowBinaryWithNamesAndTypes\n",
    header | row_binary
  ])
```

#### Insert custom [format](https://clickhouse.com/docs/en/interfaces/formats)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

csv = "0\n1"

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, ["INSERT INTO ch_demo(id) FORMAT CSV\n" | csv])
```

#### Insert [chunked](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) RowBinary stream

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

DBConnection.run(pid, fn conn ->
  Stream.repeatedly(fn -> [:rand.uniform(100)] end)
  |> Stream.chunk_every(100_000)
  |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, _types = ["UInt64"]) end)
  |> Stream.take(10)  
  |> Enum.into(Ch.stream(conn, "INSERT INTO ch_demo(id) FORMAT RowBinary\n"))
end)
```

#### Insert rows via [input](https://clickhouse.com/docs/en/sql-reference/table-functions/input) function

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

sql = "INSERT INTO ch_demo SELECT id + {ego:Int64} FROM input('id UInt64') FORMAT RowBinary\n"
row_binary = Ch.RowBinary.encode_rows([[1], [2], [3]], ["UInt64"])

%Ch.Result{num_rows: 3} =
  Ch.query!(pid, [sql | row_binary], %{"ego" => -1})
```

#### Query with custom [settings](https://clickhouse.com/docs/en/operations/settings/settings)

```elixir
{:ok, pid} = Ch.start_link()

settings = [async_insert: 1]

%Ch.Result{rows: [["async_insert", "Bool", "0"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'")

%Ch.Result{rows: [["async_insert", "Bool", "1"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'", _params = [], settings: settings)
```

## Caveats

#### NULL in RowBinary

It's the same as in [`ch-go`](https://clickhouse.com/docs/en/integrations/go#nullable)

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
rows = [[nil, nil, nil]]
row_binary = Ch.RowBinary.encode_rows(rows, types)

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, ["INSERT INTO ch_nulls(a, b, c) FORMAT RowBinary\n" | row_binary])

%Ch.Result{rows: [[nil, 0, 0]]} =
  Ch.query!(pid, "SELECT * FROM ch_nulls")
```

Note that in this example `DEFAULT 10` is ignored and `0` (the default value for `UInt8`) is persisted instead.

However, [`input()`](https://clickhouse.com/docs/en/sql-reference/table-functions/input) can be used as a workaround:

```elixir
sql = """
INSERT INTO ch_nulls
  SELECT * FROM input('a Nullable(UInt8), b Nullable(UInt8), c UInt8')
  FORMAT RowBinary
"""

types = ["Nullable(UInt8)", "Nullable(UInt8)", "UInt8"]
row_binary = Ch.RowBinary.encode_rows(rows, types)

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, [sql | row_binary])

%Ch.Result{rows: [_before = [nil, 0, 0], _after = [nil, 10, 0]]} =
  Ch.query!(pid, "SELECT * FROM ch_nulls ORDER BY b")
```

#### UTF-8 in RowBinary

When decoding [`String`](https://clickhouse.com/docs/en/sql-reference/data-types/string) columns non UTF-8 characters are replaced with `�` (U+FFFD). This behaviour is similar to [`toValidUTF8`](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#tovalidutf8) and [JSON format.](https://clickhouse.com/docs/en/interfaces/formats#json)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE ch_utf8(str String) ENGINE Memory")

# "\x61\xF0\x80\x80\x80b" will become "a�b" on SELECT
row_binary = Ch.RowBinary.encode(:string, "\x61\xF0\x80\x80\x80b")

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, ["INSERT INTO ch_utf8(str) FORMAT RowBinary\n" | row_binary])

%Ch.Result{rows: [["a�b"]]} =
  Ch.query!(pid, "SELECT * FROM ch_utf8")

%{"data" => [["a�b"]]} =
  pid |> Ch.query!("SELECT * FROM ch_utf8 FORMAT JSONCompact").data |> Jason.decode!()
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

Encoding non-UTC datetimes raises an `ArgumentError`

```elixir
Ch.query!(pid, "CREATE TABLE ch_datetimes(datetime DateTime) ENGINE Null")

naive = NaiveDateTime.utc_now()
utc = DateTime.utc_now()
taipei = DateTime.shift_zone!(utc, "Asia/Taipei")

# ** (ArgumentError) non-UTC timezones are not supported for encoding: 2023-04-26 01:49:43.044569+08:00 CST Asia/Taipei
Ch.RowBinary.encode_rows([[naive], [utc], [taipei]], ["DateTime"])
```

## Benchmarks

Please see [CI Results](https://github.com/plausible/ch/actions/workflows/bench.yml) (make sure to click the latest workflow run and scroll down to "Artifacts") for [some of our benchmarks.](./bench/) :) 
