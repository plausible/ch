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
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({$0:UInt8}), ({$1:UInt32})", [0, 1])

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({a:UInt16}), ({b:UInt64})", %{"a" => 0, "b" => 1})

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) SELECT number FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 2})
```

#### Insert rows as [RowBinary](https://clickhouse.com/docs/en/interfaces/formats#rowbinary) (efficient)

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

Similarly, you can use [`RowBinaryWithNamesAndTypes`](https://clickhouse.com/docs/en/interfaces/formats#rowbinarywithnamesandtypes) which would additionally do something like a type check.

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

Encoding non-UTC datetimes raises an `ArgumentError`

```elixir
Ch.query!(pid, "CREATE TABLE ch_datetimes(datetime DateTime) ENGINE Null")

naive = NaiveDateTime.utc_now()
utc = DateTime.utc_now()
taipei = DateTime.shift_zone!(utc, "Asia/Taipei")

# ** (ArgumentError) non-UTC timezones are not supported for encoding: 2023-04-26 01:49:43.044569+08:00 CST Asia/Taipei
Ch.query!(pid, "INSERT INTO ch_datetimes(datetime) FORMAT RowBinary", [[naive], [utc], [taipei]], types: ["DateTime"])
```

## Benchmarks

<details>
<summary><code>INSERT</code> 1 million rows <a href="https://github.com/ClickHouse/clickhouse-go#benchmark">(original)</a></summary>

<pre><code>
$ MIX_ENV=bench mix run bench/insert.exs

This benchmark is based on https://github.com/ClickHouse/clickhouse-go#benchmark

Operating System: macOS
CPU Information: Apple M1
Number of Available Cores: 8
Available memory: 8 GB
Elixir 1.14.4
Erlang 25.3

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 1_000_000 rows
Estimated total run time: 28 s

Benchmarking encode with input 1_000_000 rows ...
Benchmarking encode stream with input 1_000_000 rows ...
Benchmarking insert with input 1_000_000 rows ...
Benchmarking insert stream with input 1_000_000 rows ...

##### With input 1_000_000 rows #####
Name                    ips        average  deviation         median         99th %
encode stream          1.63      612.96 ms    ±11.30%      583.03 ms      773.01 ms
insert stream          1.22      819.82 ms     ±9.41%      798.94 ms      973.45 ms
encode                 1.09      915.75 ms    ±44.13%      750.98 ms     1637.02 ms
insert                 0.73     1373.84 ms    ±31.01%     1331.86 ms     1915.76 ms

Comparison: 
encode stream          1.63
insert stream          1.22 - 1.34x slower +206.87 ms
encode                 1.09 - 1.49x slower +302.79 ms
insert                 0.73 - 2.24x slower +760.88 ms</code>
</pre>

</details>

<details>
<summary><code>SELECT</code> 500, 500 thousand, and 500 million rows <a href="https://github.com/ClickHouse/ch-bench">(original)</a></summary>

<pre><code>
$ MIX_ENV=bench mix run bench/stream.exs

This benchmark is based on https://github.com/ClickHouse/ch-bench

Operating System: macOS
CPU Information: Apple M1
Number of Available Cores: 8
Available memory: 8 GB
Elixir 1.14.4
Erlang 25.3

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 500 rows, 500_000 rows, 500_000_000 rows
Estimated total run time: 1.05 min

Benchmarking stream with decode with input 500 rows ...
Benchmarking stream with decode with input 500_000 rows ...
Benchmarking stream with decode with input 500_000_000 rows ...
Benchmarking stream with manual decode with input 500 rows ...
Benchmarking stream with manual decode with input 500_000 rows ...
Benchmarking stream with manual decode with input 500_000_000 rows ...
Benchmarking stream without decode with input 500 rows ...
Benchmarking stream without decode with input 500_000 rows ...
Benchmarking stream without decode with input 500_000_000 rows ...

##### With input 500 rows #####
Name                                ips        average  deviation         median         99th %
stream with decode               4.69 K      213.34 μs    ±12.49%      211.38 μs      290.94 μs
stream with manual decode        4.69 K      213.43 μs    ±17.40%      210.96 μs      298.75 μs
stream without decode            4.65 K      215.08 μs    ±10.79%      213.79 μs      284.66 μs

Comparison:
stream with decode               4.69 K
stream with manual decode        4.69 K - 1.00x slower +0.0838 μs
stream without decode            4.65 K - 1.01x slower +1.74 μs

##### With input 500_000 rows #####
Name                                ips        average  deviation         median         99th %
stream without decode            234.58        4.26 ms    ±13.99%        4.04 ms        5.95 ms
stream with manual decode         64.26       15.56 ms     ±8.36%       15.86 ms       17.97 ms
stream with decode                41.03       24.37 ms     ±6.27%       24.39 ms       26.60 ms

Comparison:
stream without decode            234.58
stream with manual decode         64.26 - 3.65x slower +11.30 ms
stream with decode                41.03 - 5.72x slower +20.11 ms

##### With input 500_000_000 rows #####
Name                                ips        average  deviation         median         99th %
stream without decode              0.32         3.17 s     ±0.20%         3.17 s         3.17 s
stream with manual decode        0.0891        11.23 s     ±0.00%        11.23 s        11.23 s
stream with decode               0.0462        21.66 s     ±0.00%        21.66 s        21.66 s

Comparison:
stream without decode              0.32
stream with manual decode        0.0891 - 3.55x slower +8.06 s
stream with decode               0.0462 - 6.84x slower +18.50 s</code>
</pre>

</details>

[CI Results](https://github.com/plausible/ch/actions/workflows/bench.yml) (click the latest workflow run and scroll down to "Artifacts")
