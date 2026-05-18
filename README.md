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
%Ch.Result{names: ["number"], rows: [[0], [1] | _rest]} =
  Ch.query!(
    pool,
    "select number from numbers({limit:UInt32})",
    %{"limit" => 100},
    headers: [{"accept-encoding", "zstd"}]
  )
```

Create a table and insert RowBinary data:

```elixir
session_id = "ch-demo-session"

Ch.query!(
  pool,
  "create temporary table demo(id UInt64, text String) engine Memory",
  %{},
  settings: %{"session_id" => session_id}
)

names = ["id", "text"]
types = ["UInt64", "String"]
rows = [[1, "one"], [2, "two"]]

insert = [
  "INSERT INTO demo FORMAT RowBinaryWithNamesAndTypes\n",
  Ch.RowBinary.encode_names_and_types(names, types)
  | Ch.RowBinary.encode_rows(rows, types)
]

Ch.query!(
  pool,
  :zstd.compress(insert),
  %{},
  settings: %{"session_id" => session_id},
  headers: [{"content-encoding", "zstd"}]
)
```
