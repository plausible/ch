# Ch

[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/ch)
[![Hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/ch)
[![Coveralls](https://img.shields.io/coverallsCoverage/github/plausible/ch?branch=master&style=flat&label=Coverage)](https://coveralls.io/github/plausible/ch?branch=master)

HTTP [ClickHouse](https://clickhouse.com) client for Elixir.

Used in [Ecto ClickHouse adapter](https://github.com/plausible/ecto_ch).

### Key features

- RowBinary
- Native query parameters
- Per query settings

## Installation

```elixir
defp deps do
  [
    {:ch, "~> 0.9.0"}
  ]
end
```

## Usage

Start a pool:

```elixir
{:ok, pool} = Ch.start_link(url: "http://localhost:8123")
```

Run a query with named ClickHouse parameters:

```elixir
Ch.query!(
  pool,
  "SELECT {limit:UInt64}",
  %{"limit" => 42}
)
```

Positional parameters such as `{$0:UInt64}` are no longer supported. Use named parameters instead:

```elixir
# before
Ch.query!(pool, "SELECT {$0:UInt64}", [42])

# now
Ch.query!(pool, "SELECT {value:UInt64}", %{"value" => 42})
```

By default, `Ch.query/4` requests `RowBinaryWithNamesAndTypes` and returns a decoded result:

```elixir
%Ch.Result{
  names: ["number"],
  rows: [[42]],
  headers: headers,
  data: raw_body
} = Ch.query!(pool, "SELECT 42 AS number")
```

To get a raw CSV or JSON response, override the ClickHouse response format and read `data`:

```elixir
%Ch.Result{data: csv} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 3",
    %{},
    headers: [{"x-clickhouse-format", "CSV"}]
  )

%Ch.Result{data: json} =
  Ch.query!(
    pool,
    "SELECT number FROM system.numbers LIMIT 3",
    %{},
    headers: [{"x-clickhouse-format", "JSONEachRow"}]
  )
```

Insert RowBinary data by encoding it explicitly:

```elixir
rows = [[1, "one"], [2, "two"]]
types = ["UInt8", "String"]
rowbinary = Ch.RowBinary.encode_rows(rows, types)

Ch.query!(pool, [
  "INSERT INTO events FORMAT RowBinary\n",
  rowbinary
])
```

Compressed inserts use the same shape, with the whole request body compressed:

```elixir
names = ["id", "name"]
types = ["UInt8", "String"]
rows = [[1, "one"], [2, "two"]]

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
