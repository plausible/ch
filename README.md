Minimal HTTP ClickHouse client.

**tl;dr**

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

# https://clickhouse.com/docs/en/quick-start#step-3-create-a-database-and-table
iex> {:ok, conn} = Ch.start_link(scheme: "http", hostname: "localhost", port: 8123)
iex> {:ok, _} = Ch.query(conn, "CREATE DATABASE IF NOT EXISTS helloworld")
iex> {:ok, _} = Ch.query(conn, """
CREATE TABLE helloworld.my_first_table
(
    user_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree()
PRIMARY KEY (user_id, timestamp)
""")

iex> types = [:u32, :string, :datetime, :f32]
iex> rows = [
  [101, "Hello, ClickHouse!", ~N[2023-01-06 03:50:38], -1.0],
  [102, "Insert a lot of rows per batch", ~N[2023-01-05 00:00:00], 1.41421],
  [102, "Sort your data based on your commonly-used queries", ~N[2023-01-06 00:00:00], 2.718],
  [101, "Granules are the smallest chunks of data read", ~N[2023-01-06 03:55:38], 3.14159]
]

iex> {:ok, %{num_rows: 4}} = Ch.query(conn, "INSERT INTO helloworld.my_first_table(user_id, message, timestamp, metric)", rows, types: types)

iex> Ch.query(conn, "SELECT * FROM helloworld.my_first_table")
{:ok,
 %{
   num_rows: 4,
   rows: [
     [101, "Hello, ClickHouse!", ~N[2023-01-06 03:50:38], -1.0],
     [101, "Granules are the smallest chunks of data read", ~N[2023-01-06 03:55:38], 3.141590118408203],
     [102, "Insert a lot of rows per batch", ~N[2023-01-05 00:00:00], 1.4142099618911743],
     [102, "Sort your data based on your commonly-used queries", ~N[2023-01-06 00:00:00], 2.7179999351501465]
   ]
 }}
```

### Examples

- `SELECT` with params

```elixir
# https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters
statement = "SELECT {a:Array(UInt8)}, {b:UInt8}, {c:String}, {d:DateTime}"
params = %{a: [1,2], b: 123, c: "123", d: ~N[2023-01-06 03:06:32]}
{:ok, %{num_rows: 1, rows: [[[1, 2], 123, "123", ~N[2023-01-06 03:06:32]]]}} = Ch.query(conn, statement, params)
```

- `INSERT` a `CSV` file stream

```elixir
{:ok, _} = Ch.query(conn, "CREATE TABLE example(a UInt32, b String) ENGINE=Memory")
```

```elixir
csv = """
1,a
2,b
3,c\
"""

File.write!("example.csv", csv)
{:ok, _} = Ch.query(conn, "INSERT INTO example(a, b)", File.stream!("example.csv"), format: "CSV")
```

- `INSERT` a `CSVWithNames` file stream

```elixir
csv = """
a,b
1,a
2,b
3,c\
"""

File.write!("example.csv", csv)
{:ok, _} = Ch.query(conn, "INSERT INTO example", File.stream!("example.csv"), format: "CSVWithNames")
```

- custom [settings](https://clickhouse.com/docs/en/operations/settings/)

```elixir
iex> Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 1])
{:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}}
```
