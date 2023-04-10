# Ch

[![Hex Package](https://img.shields.io/hexpm/v/before_ch.svg)](https://hex.pm/packages/before_ch)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/before_ch)

ClickHouse driver for Elixir.

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.1.0"}
  ]
end
```

## Examples

```iex
iex> {:ok, pid} = Ch.start_link(scheme: "http", hostname: "localhost", port: 8123, database: "default")
{:ok, #PID<0.269.0>}

iex> Ch.query!(pid, "SELECT number, number * 2 FROM system.numbers LIMIT 2")
%Ch.Result{command: :select, rows: [[0, 0], [1, 2]]}

iex> Ch.query!(pid, "CREATE TABLE my_first_table (metric Float32) ENGINE = Memory")
%Ch.Result{command: :create, rows: []}

iex> Ch.query!(pid, "INSERT INTO my_first_table VALUES (0), (1)")
%Ch.Result{command: :insert, num_rows: 2}
```

## Features

- [native parameters](https://clickhouse.com/docs/en/interfaces/cli#cli-queries-with-parameters) in queries

```iex
iex> Ch.query!(pid, "SELECT {$0:String}, {$1:Int64}", ["hello", 123])
%Ch.Result{command: :select, rows: [["hello", 123]]}
```

- custom [formats](https://clickhouse.com/docs/en/sql-reference/formats), default format for reads is `RowBinaryWithNamesAndTypes`

```iex
iex> Ch.query!(pid, "SELECT 1, 'text' FORMAT CSVWithNames")
%Ch.Result{command: :select, rows: ["\"1\",\"'text'\"\n", "1,\"text\"\n"]}

iex> Ch.query!(pid, "SELECT 1, 'text'", [], format: "CSVWithNames")
%Ch.Result{command: :select, rows: ["\"1\",\"'text'\"\n", "1,\"text\"\n"]}

# equivalent to default
iex> Ch.query!(pid, "SELECT 1, 'text'", [], format: "RowBinaryWithNamesAndTypes")
%Ch.Result{command: :select, rows: [[1, "text"]]}

iex> Ch.query!(pid, "SELECT 1, 'text' FORMAT CSV", [], format: "RowBinaryWithNamesAndTypes")
%Ch.Result{command: :select, rows: ["1,\"text\"\n"]}
```

- efficient `RowBinary` encoder

```iex
iex> rows = [[1], [2.0], [0.0]]
iex> types = [:f32]
iex> data = Ch.RowBinary.encode_rows(rows, types)
iex> Ch.query!(pid, "INSERT INTO my_first_table (metric) FORMAT RowBinary", {:raw, data})
%Ch.Result{command: :insert, num_rows: 3}
```

- streaming inserts

```iex
iex> rows = Stream.repeatedly(fn -> [:rand.uniform()] end) |> Stream.take(1000)
iex> types = [:f32]
iex> stream = rows |> Stream.chunk_every(100) |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, types) end)
iex> Ch.query!(pid, "INSERT INTO my_first_table (metric) FORMAT RowBinary", stream)
%Ch.Result{command: :insert, num_rows: 1000}
```

- [settings](https://clickhouse.com/docs/en/operations/settings)

```iex
iex> Ch.query(pid, "INSERT INTO my_first_table VALUES (0), (1)", [], settings: [readonly: 1])
{:error,
 %Ch.Error{
   code: 164,
   message: "Code: 164. DB::Exception: default: Cannot execute query in readonly mode. (READONLY)"
 }}

iex> Ch.query!(pid, "SHOW SETTINGS LIKE 'async_insert'", [], settings: [async_insert: 1])
%Ch.Result{command: :show, rows: [["async_insert", "Bool", "1"]]}
```

## Caveats and limitations

### Nullable

Inserting `nil` into a `Nullable` column results in `NULL`.
In all other cases the default value for the **type** is persisted.

```iex
iex> Ch.query(pid, "CREATE TABLE my_nulls (a UInt8 NULL, b UInt8 DEFAULT 10, c UInt8 NOT NULL) ENGINE = Memory")

iex> rows = [[nil, nil, nil], [1, 1, 1]]
iex> types = [{:nullable, :u8}, :u8, :u8]
iex> Ch.query!(pid, "INSERT INTO my_nulls(a, b, c) FORMAT RowBinary", {:raw, Ch.RowBinary.encode_rows(rows, types)})
%Ch.Result{command: :insert, num_rows: 2}

# b UInt8 DEFAULT 10 is ignored
iex> Ch.query!(pid, "SELECT * FROM my_nulls")
%Ch.Result{command: :select, rows: [[nil, 0, 0], [1, 1, 1]]}
```

## Alternatives

- [Pillar](https://github.com/)

## Benchmarks

```
$ curl -O https://
$ elixir ch_benchmark.exs
```

## License
