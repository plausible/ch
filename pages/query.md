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

For hot insert paths, define the schema once and generate a row or insert-body
encoder:

```elixir
defmodule EventInsert do
  require Ch.RowBinary

  Ch.RowBinary.define_encoder(
    schema: [id: "UInt8", name: "String"],
    name: :encode_insert,
    table: "events"
  )
end

Ch.query!(pool, EventInsert.encode_insert([%{id: 1, name: "one"}]))
```

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
