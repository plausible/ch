# Ch

[![Hex Package](https://img.shields.io/hexpm/v/ch.svg)](https://hex.pm/packages/ch)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/ch)

Minimal HTTP ClickHouse client for Elixir.

Used in [Ecto ClickHouse adapter.](https://github.com/plausible/chto)

### Key features

- RowBinary
- Native query parameters
- Per query settings

## Installation

```elixir
defp deps do
  [
    {:ch, github: "plausible/ch"}
  ]
end
```

## Usage

#### Start [DBConnection](https://github.com/elixir-ecto/db_connection) pool

```elixir
ch_defaults = [
  scheme: "http",
  hostname: "localhost",
  port: 8123,
  database: "default",
  settings: []
]

db_connection_defaults = [
  pool_size: 1,
  timeout: 15_000
]

{:ok, pid} = Ch.start_link(ch_defaults ++ db_connection_defaults)
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
- `%DateTime{}` (non UTC) raises an error

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

#### Insert rows as [RowBinary](https://clickhouse.com/docs/en/interfaces/formats#rowbinary) (recommended)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT RowBinary", [[0], [1]], types: [:u64])
```

Note that RowBinary format encoding requires `:types` option to be provided.

#### Types

| ClickHouse                                                                                                                                                                           | Ch                                                                                                                                                             | Elixir                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| [UInt8<br>UInt16<br>UInt32<br>UInt64<br>UInt128<br>UInt256<br>Int8<br>Int16<br>Int32<br>Int64<br>Int128<br>Int256](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint) | `:u8`<br>`:u16`<br>`:u32`<br>`:u64`<br>`:u128`<br>`:u256`<br>`:i8`<br>`:i16`<br>`:i32`<br>`:i64`<br>`:i128`<br>`:i256`                                         | `42`                                              |
| [Float32<br>Float64](https://clickhouse.com/docs/en/sql-reference/data-types/float)                                                                                                  | `:f32`<br>`:f64`                                                                                                                                               | `42.0`                                            |
| [Decimal(P, S)<br>Decimal32(S)<br>Decimal64(S)<br>Decimal128(S)<br>Decimal256(S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)                                   | `{:decimal, p = 18, s = 2}`<br>`{:decimal32, s = 2}`<br>`{:decimal64, s = 2}`<br>`{:decimal128, s = 2}`<br>`{:decimal256, s = 2}`                              | [`%Decimal{}`](https://github.com/ericmj/decimal) |
| [Bool](https://clickhouse.com/docs/en/sql-reference/data-types/boolean)                                                                                                              | `:boolean`                                                                                                                                                     | `true`                                            |
| [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)                                                                                                             | `:string`<br>`:binary` \*                                                                                                                                      | `"José"`<br>`<<0,1,2>>`                           |
| [FixedString(N)](https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring)                                                                                                | `{:fixed_string, n = 3}`<br>`{:fixed_binary, n = 3}` \*\*                                                                                                      | `"BR"`<br>`<<0,1,2>>`                             |
| [UUID](https://clickhouse.com/docs/en/sql-reference/data-types/uuid)                                                                                                                 | `:uuid`                                                                                                                                                        | `<<0::128>>`                                      |
| [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date)                                                                                                                 | `:date`                                                                                                                                                        | `%Date{}`                                         |
| [Date32](https://clickhouse.com/docs/en/sql-reference/data-types/date32)                                                                                                             | `:date32`                                                                                                                                                      | `%Date{}`                                         |
| [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)                                                                                                         | `:datetime`                                                                                                                                                    | `%NaiveDateTime{}`                                |
| [DateTime(timezone)](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)                                                                                               | `{:datetime, tz = "Asia/Tokyo"}` \*\*\*                                                                                                                        | `%DateTime{}`                                     |
| [DateTime64(precision)]()                                                                                                                                                            | `{:datetime64, precision = 3}`                                                                                                                                 | `%NaiveDateTime{}`                                |
| [DateTime64(precision, timezone)]()                                                                                                                                                  | `{:datetime64, p = 3, tz = "Asia/Tokyo"}` \*\*\*                                                                                                               | `%DateTime{}`                                     |
| [Enum('hello' = 1, 'world' = 2)<br>Enum8('hello' = 1, 'world' = 2)<br>Enum16('hello' = 1, 'world' = 2)](https://clickhouse.com/docs/en/sql-reference/data-types/enum)                | `{:enum8, hello: 1, world: 2}`<br>`{:enum16, hello: 1, world: 2}`<br><br>`{:enum8, [{"hello", 1}, {"world", 2}]}`<br>`{:enum16, [{"hello", 1}, {"world", 2}]}` | `:hello`<br>`"hello"`                             |
| [Array(T)](https://clickhouse.com/docs/en/sql-reference/data-types/array)                                                                                                            | `{:array, t = :i8}`                                                                                                                                            | `[1,2,3]`                                         |
| [Tuple(T1, T2, ...)](https://clickhouse.com/docs/en/sql-reference/data-types/tuple)                                                                                                  | `{:tuple, [t1 = :i8, t2 = :string, ...]}`                                                                                                                      | `{1,"2",...}`                                     |
| [Map(key, value)](https://clickhouse.com/docs/en/sql-reference/data-types/map)                                                                                                       | `{:map, k = :string, v = :u8}`                                                                                                                                 | `%{"answer" => 42}`                               |
| [Nullable(T)](https://clickhouse.com/docs/en/sql-reference/data-types/nullable)                                                                                                      | `{:nullable, t = :u8}`                                                                                                                                         | `nil`                                             |
| [IPv4](https://clickhouse.com/docs/en/sql-reference/data-types/domains/ipv4)                                                                                                         | `:ipv4`                                                                                                                                                        | `{127,0,0,1}`                                     |
| [IPv6](https://clickhouse.com/docs/en/sql-reference/data-types/domains/ipv6)                                                                                                         | `:ipv6`                                                                                                                                                        | `{0,0,0,0,0,0,0,1}`                               |
| [Point](https://clickhouse.com/docs/en/sql-reference/data-types/geo#point)                                                                                                           | `:point`                                                                                                                                                       | `{4,2}`                                           |
| [Ring](https://clickhouse.com/docs/en/sql-reference/data-types/geo#ring)                                                                                                             | `:ring`                                                                                                                                                        | `[{4,2}]`                                         |
| [Polygon](https://clickhouse.com/docs/en/sql-reference/data-types/geo#polygon)                                                                                                       | `:polygon`                                                                                                                                                     | `[[{4,2}]]`                                       |
| [MultiPolygon](https://clickhouse.com/docs/en/sql-reference/data-types/geo#multipolygon)                                                                                             | `:multipolygon`                                                                                                                                                | `[[[{4,2}]]]`                                     |

\*\*\*\* decoding requires a [timezone database;](https://hexdocs.pm/elixir/DateTime.html#module-time-zone-database) encoding raises for non-UTC timezone

#### Insert rows in custom [format](https://clickhouse.com/docs/en/interfaces/formats)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

csv = [0, 1] |> Enum.map(&to_string/1) |> Enum.intersperse(?\n)

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT CSV", {:raw, csv})
```

#### Insert rows as chunked RowBinary stream

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

stream = Stream.repeatedly(fn -> [:rand.uniform(100)] end)
chunked = Stream.chunk_every(stream, 100)
encoded = Stream.map(chunked, fn chunk -> Ch.RowBinary.encode_rows(chunk, [:u64]) end)
ten_encoded_chunks = Stream.take(encoded, 10)

%Ch.Result{num_rows: 1000} =
  Ch.query(pid, "INSERT INTO ch_demo(id) FORMAT RowBinary", {:raw, ten_encoded_chunks})
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
) ENGINE = Memory
""")

types = [{:nullable, :u8}, :u8, :u8]
inserted_rows = [[nil, nil, nil]]
selected_rows = [[nil, 0, 0]]

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_nulls(a, b, c) FORMAT RowBinary", inserted_rows, types: types)

%Ch.Result{rows: ^selected_rows} =
  Ch.query!(pid, "SELECT * FROM ch_nulls")
```

Note that in this example `DEFAULT 10` is ignored and `0` (the default value for `UInt8`) is stored instead.

#### UTF-8 in RowBinary

When decoding [`String`](https://clickhouse.com/docs/en/sql-reference/data-types/string) columns or `:string` types (if manually provided in `:types` option), non UTF-8 characters are replaced with `�` (U+FFFD). This behaviour is similar to [`toValidUTF8`](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#tovalidutf8) and [JSON formats.](https://clickhouse.com/docs/en/interfaces/formats#json)

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE ch_utf8(str String) ENGINE = Memory")

raw = "\x61\xF0\x80\x80\x80b"
utf8 = "a�b"

%Ch.Result{num_rows: 1} =
  Ch.query!(pid, "INSERT INTO ch_utf8(str) FORMAT RowBinary", [[raw]], types: [:string])

%Ch.Result{rows: [[^utf8]]} =
  Ch.query!(pid, "SELECT * FROM ch_utf8")

%Ch.Result{rows: %{"data" => [[^utf8]]}} =
  pid |> Ch.query!("SELECT * FROM ch_utf8 FORMAT JSONCompact") |> Map.update!(:rows, &Jason.decode!/1)
```

To get raw binary use `:binary` type that skips UTF-8 checks.

```elixir
%Ch.Result{rows: [[^raw]]} =
  Ch.query!(pid, "SELECT * FROM ch_utf8", [], types: [:binary])
```

## Benchmarks

<details>
<summary><code>INSERT</code> 1 million rows <a href="https://github.com/ClickHouse/clickhouse-go#benchmark">(original)</a></summary>

<pre><code>
$ MIX_ENV=bench mix run bench/insert_stream.exs

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
inputs: medium (1_000_000 rows)
Estimated total run time: 21 s

Benchmarking control with input medium (1_000_000 rows) ...
Benchmarking encode with input medium (1_000_000 rows) ...
Benchmarking insert with input medium (1_000_000 rows) ...

##### With input medium (1_000_000 rows) #####
Name              ips        average  deviation         median         99th %
control          2.62      381.00 ms     ±4.13%      376.77 ms      412.36 ms
encode           1.03      969.49 ms     ±4.00%      965.70 ms     1030.67 ms
insert           0.89     1127.15 ms     ±3.52%     1110.76 ms     1183.62 ms

Comparison:
control          2.62
encode           1.03 - 2.54x slower +588.49 ms
insert           0.89 - 2.96x slower +746.15 ms</code>
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
