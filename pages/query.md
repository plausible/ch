# Querying

`Ch.query/4` sends SQL to ClickHouse over HTTP.

By default, Ch asks ClickHouse for `RowBinaryWithNamesAndTypes`, decodes the response, and returns `%Ch.Result{}`:

```elixir
%Ch.Result{
  names: ["number"],
  rows: [[42]],
  headers: headers,
  data: raw_body
} = Ch.query!(pool, "SELECT 42 AS number")
```

## Named Parameters

Query parameters are named. The map keys do not include ClickHouse's `param_` prefix:

```elixir
Ch.query!(
  pool,
  "SELECT {value:UInt64}",
  %{"value" => 42}
)
```

Positional parameters are not supported:

```elixir
# before
Ch.query!(pool, "SELECT {$0:UInt64}", [42])

# now
Ch.query!(pool, "SELECT {value:UInt64}", %{"value" => 42})
```

Use the same naming style for multiple parameters:

```elixir
Ch.query!(
  pool,
  "SELECT {name:String}, {age:UInt8}",
  %{"name" => "Ada", "age" => 37}
)
```

## Raw Formats

The default response format is decoded. To receive raw CSV, JSON, TSV, or another ClickHouse format, override the `x-clickhouse-format` header:

```elixir
%Ch.Result{data: csv} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 3",
    %{},
    headers: [{"x-clickhouse-format", "CSV"}]
  )
```

```elixir
%Ch.Result{data: json_each_row} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 3",
    %{},
    headers: [{"x-clickhouse-format", "JSONEachRow"}]
  )
```

For raw successful responses, Ch returns `%Ch.Result{}` with the body as received in `data`. It does not decode rows or decompress compressed raw responses.

```elixir
%Ch.Result{names: nil, rows: nil, data: csv} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 3",
    %{},
    headers: [{"x-clickhouse-format", "CSV"}]
  )
```

## RowBinary Inserts

RowBinary inserts are explicit. Encode rows with `Ch.RowBinary` and pass the SQL plus encoded data as the request body:

```elixir
rows = [[1, "one"], [2, "two"]]
types = ["UInt8", "String"]
rowbinary = Ch.RowBinary.encode_rows(rows, types)

Ch.query!(pool, [
  "INSERT INTO events FORMAT RowBinary\n",
  rowbinary
])
```

For `RowBinaryWithNamesAndTypes`, include the encoded names and types header:

```elixir
names = ["id", "name"]
types = ["UInt8", "String"]
rows = [[1, "one"], [2, "two"]]

Ch.query!(pool, [
  "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types),
  Ch.RowBinary.encode_rows(rows, types)
])
```

If the same types are used repeatedly, prepare them once to avoid reparsing and
normalizing the schema for every batch:

```elixir
plan = Ch.RowBinary.prepare(["UInt8", "String"])
rowbinary = Ch.RowBinary.encode_rows(rows, plan)
decoded_rows = Ch.RowBinary.decode_rows(IO.iodata_to_binary(rowbinary), plan)
```

### Generated Encoders

For fixed schemas on hot insert paths, `Ch.RowBinary.Encoder.define_encoder/1`
generates direct encoding code for common types at compile time. It bypasses
generic type dispatch while using the same type-specific helpers as the generic
codec:

```elixir
defmodule EventInsert do
  require Ch.RowBinary.Encoder

  Ch.RowBinary.Encoder.define_encoder(
    name: :encode,
    schema: [id: "UInt64", name: "String"],
    table: "events"
  )
end

Ch.query!(pool, EventInsert.encode([%{id: 1, name: "one"}]))
```

List schemas preserve their declared column order. Map schemas are sorted by
column name. This order controls encoded fields and the names/types header.
Map/struct rows are the default; set `row: :list` for list rows in schema order.
Set `rows: true` for batches without an insert header. A `table:` option implies
batch encoding and accepts only dot-separated unquoted identifiers.

Generated code is duplicated in every caller module for every schema. Reserve
it for measured hot paths and track caller BEAM size and compile time as schemas
are added; use `Ch.RowBinary.prepare/1` for lower-complexity reuse.

Run `mix run dev/row_binary_encoder_benchmark.exs` to compare generated and
generic encoders and report generated-module compile time and BEAM size. Set the
`ROWS` and `ITERATIONS` environment variables to change the default
one-million-row workload and three samples. The list-row cases use identical
inputs and a prepared generic plan, isolating codec dispatch; map-field
extraction is reported separately.

## Compressed Inserts

ClickHouse accepts compressed request bodies when the `content-encoding` header is set. Compress the entire SQL plus data body:

```elixir
payload =
  :zstd.compress([
    "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
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
