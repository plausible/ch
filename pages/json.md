# JSON

ClickHouse 24.1+ has a native [`JSON` column type](https://clickhouse.com/docs/sql-reference/data-types/newjson)
that stores semi-structured data with automatic path inference and typed storage.
This guide covers how to use it with `RowBinaryWithNamesAndTypes` via the
[RowBinary format settings](https://clickhouse.com/docs/interfaces/formats/RowBinary#format-settings).

## Required settings

`Ch.RowBinary.encode(:json, value)` serializes JSON values as RowBinary strings containing
JSON text (using `JSON.encode_to_iodata!/1` internally). ClickHouse's default RowBinary
wire format for `JSON` columns is a complex internal binary encoding — you must opt into
the string representation on both sides:

| Direction | Setting |
|---|---|
| INSERT | `input_format_binary_read_json_as_string: true` |
| SELECT | `output_format_binary_write_json_as_string: true` |

Pass them via `Ch.HTTP.path/2`:

```elixir
# SELECT
path = Ch.HTTP.path(%{}, output_format_binary_write_json_as_string: true)

# INSERT
path = Ch.HTTP.path(%{}, input_format_binary_read_json_as_string: true)
```

## INSERT

Pass Elixir maps or lists directly — the library calls `JSON.encode_to_iodata!/1`
internally, no manual encoding needed:

```elixir
rows = [
  [1, %{"action" => "click", "element" => "button"}],
  [2, %{"action" => "view", "page" => "/home"}]
]

types = ["UInt64", "JSON"]
names = ["id", "data"]

body = [
  "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types),
  Ch.RowBinary.encode_rows(rows, types)
]

path = Ch.HTTP.path(%{}, input_format_binary_read_json_as_string: true)

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, headers, body)
```

## SELECT

```elixir
path = Ch.HTTP.path(%{}, output_format_binary_write_json_as_string: true)

{:ok, _ref, conn} = Mint.HTTP1.request(conn, "POST", path, headers, body)
{:ok, conn, responses} = Mint.HTTP1.recv(conn, 0, 5_000)

state = Ch.HTTP.decode_start()
{_state, rows} = Enum.reduce(responses, {state, []}, fn response, {state, acc} ->
  case Ch.HTTP.decode_continue(state, response) do
    {:rows, rows, _names, state} -> {state, acc ++ rows}
    {:cont, state} -> {state, acc}
    {:ok, _names, rows} -> {state, acc ++ rows}
    _ -> {state, acc}
  end
end)
# rows => [[1, %{"action" => "click", "element" => "button"}], ...]
```

`Ch.HTTP` streaming decoder feeds `Ch.RowBinary.decode_rows_continue/3`, which decodes RowBinary strings
back into Elixir maps using `JSON.decode!/1` for the `:json` type.

## Typed paths

The `JSON` column type optionally accepts type hints for known paths, which ClickHouse
stores with fixed types rather than inferred ones:

```sql
CREATE TABLE events (
  id   UInt64,
  data JSON(
    action LowCardinality(String),
    ts     DateTime64(3, 'UTC')
  )
) ENGINE = MergeTree ORDER BY id
```

Typed paths don't change the wire format — the same string encoding applies. The values
for typed paths just need to match their declared types.

## Performance note

`Ch.RowBinary.encode_rows/2` calls `encoding_types/1` on every invocation to decode
type strings (`"UInt64"`, `"JSON"`) into internal atoms. If you're inserting in a tight
loop across many batches, this is repeated work. A future `Ch.Buffer` struct would cache
the pre-decoded types alongside accumulated rows to avoid this overhead.

Until then, you can call `Ch.RowBinary.encoding_types/1` once and reuse the result across
batches by using the internal `Ch.RowBinary._encode_rows/2`.

## Format settings reference

See [RowBinary format settings](https://clickhouse.com/docs/interfaces/formats/RowBinary#format-settings).

| Setting | Default | Effect |
|---|---|---|
| `output_format_binary_write_json_as_string` | `false` | Write JSON columns as JSON strings in SELECT |
| `input_format_binary_read_json_as_string` | `false` | Read JSON columns from JSON strings in INSERT |
| `format_binary_max_string_size` | `1GiB` | Maximum string length in RowBinary |

## Tests

See [`test/ch/guides/json_test.exs`](../test/ch/guides/json_test.exs).
