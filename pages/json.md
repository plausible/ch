# JSON

ClickHouse has two distinct JSON-related features: storing JSON in a `String` column,
and the native `JSON` type (ClickHouse ≥ 24.1). Both are supported via `RowBinaryWithNamesAndTypes`.

## JSON stored in String columns

Store serialised JSON as a `String`. Encode with `JSON.encode!/1` (Elixir ≥ 1.18 stdlib):

```elixir
types = ["UInt64", "String"]
names = ["id", "metadata"]

rows = [
  [1, JSON.encode!(%{source: "web", browser: "Firefox"})],
  [2, JSON.encode!(%{source: "mobile", os: "iOS"})]
]

body = [
  "INSERT INTO events FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types),
  Ch.RowBinary.encode_rows(rows, types)
]
```

Query it back with ClickHouse JSON functions:

```sql
SELECT id, JSONExtractString(metadata, 'source') FROM events
```

## Native JSON type (ClickHouse ≥ 24.1)

The `JSON` column type stores semi-structured data with automatic column extraction.
In `RowBinaryWithNamesAndTypes`, `JSON` columns are encoded and decoded as Elixir
maps or lists — the same as any other term:

```elixir
types = ["UInt64", "JSON"]
names = ["id", "data"]

rows = [
  [1, %{"action" => "click", "element" => "button"}],
  [2, %{"action" => "view", "page" => "/home"}]
]

header = Ch.RowBinary.encode_names_and_types(names, types)
encoded = Ch.RowBinary.encode_rows(rows, types)
```

Decoding a response with `JSON` columns returns Elixir maps:

```elixir
{:ok, names, rows} = Ch.HTTP.decode(status, headers, body)
# rows => [[1, %{"action" => "click", ...}], ...]
```

## Querying JSON sub-fields

To extract JSON fields server-side (avoiding transferring full JSON blobs), use
ClickHouse's `JSON_VALUE`, `JSONExtract*`, or the `.field` accessor syntax for the
native `JSON` type:

```sql
SELECT id, data.action FROM events
-- returns rows where data.action is a String
```

The resulting `RowBinaryWithNamesAndTypes` response will have the extracted field
as a `String` (or inferred type) column — decode normally.

## Tests

See [`test/ch/guides/json_test.exs`](../test/ch/guides/json_test.exs).
