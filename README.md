# Ch

Minimal HTTP ClickHouse client for Elixir. Used as the driver for a [ClickHouse Ecto adapter.](https://github.com/plausible/chto)

## Installation

```elixir
defp deps do
  [
    {:ch, github: "plausible/ch"}
  ]
end
```

## Usage

<details>
<summary>Start <code>DBConnection</code> pool</summary>

```elixir
ch_defaults = [
  scheme: "http",
  hostname: "localhost",
  port: 8123,
  database: "default",
  settings: []
]

dbconnection_defaults = [
  pool_size: 1
]

{:ok, pid} = Ch.start_link(ch_defaults ++ dbconnection_defaults)
```

</details>

<details>
<summary>Select rows</summary>

```elixir
{:ok, pid} = Ch.start_link()

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT 3")

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT {$0:UInt8}", [3])

{:ok, %Ch.Result{rows: [[0], [1], [2]]}} =
  Ch.query(pid, "SELECT * FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 3})
```

</details>

<details>
<summary>Insert rows</summary>

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES (0), (1)")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({$0:UInt8}), ({$0:UInt32})", [0, 1])

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) VALUES ({a:UInt16}), ({b:UInt64})", %{"a" => 0, "b" => 1})

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) SELECT number FROM system.numbers LIMIT {limit:UInt8}", %{"limit" => 2})
```

</details>

<details>
<summary>Insert RowBinary</summary>

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT RowBinary", [[0], [1]], types: [:u64])
```

</details>

<details>
<summary>Insert custom format</summary>

```elixir
{:ok, pid} = Ch.start_link()

Ch.query!(pid, "CREATE TABLE IF NOT EXISTS ch_demo(id UInt64) ENGINE Null")

csv = [0, 1] |> Enum.map(&to_string/1) |> Enum.intersperse(?\n)

%Ch.Result{num_rows: 2} =
  Ch.query!(pid, "INSERT INTO ch_demo(id) FORMAT CSV", {:raw, csv})
```

</details>

<details>
<summary>Insert stream</summary>

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

</details>

<details>
<summary>Query with settings</summary>

```elixir
{:ok, pid} = Ch.start_link()

%Ch.Result{rows: [["async_insert", "Bool", "0"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'")

%Ch.Result{rows: [["async_insert", "Bool", "1"]]} =
  Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'", [], settings: [async_insert: 1])
```

</details>

## Benchmarks

<details>
<summary>Insert 1 million rows</summary>

```console
$ MIX_ENV=bench mix run bench/insert_stream.exs

This benchmarks is based on https://github.com/ClickHouse/clickhouse-go#benchmark

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
insert           0.89 - 2.96x slower +746.15 ms
```

</details>

<details>
<summary>Select 500, 500 thousand, and 500 million rows</summary>

```console
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
stream with decode               0.0462 - 6.84x slower +18.50 s
```

</details>

<sub>[CI Results](https://github.com/plausible/ch/actions/workflows/bench.yml) (click the latest workflow run and scroll down to "Artifacts")</sub>
