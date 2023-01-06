Minimal HTTP ClickHouse client.

**tl;dr**

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

# https://clickhouse.com/docs/en/quick-start#step-3-create-a-database-and-table
iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123)
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

iex> rows = Stream.map(
  [
    [101, "Hello, ClickHouse!", ~N[2023-01-06 03:50:38], -1.0],
    [102, "Insert a lot of rows per batch", ~N[2023-01-05 00:00:00], 1.41421],
    [102, "Sort your data based on your commonly-used queries", ~N[2023-01-06 00:00:00], 2.718],
    [101, "Granules are the smallest chunks of data read", ~N[2023-01-06 03:55:38], 3.14159]
  ],
  fn row -> Ch.encode_row_binary(row, [:u32, :string, :datetime, :f32]) end
)

iex> {:ok, %{num_rows: 4}} = Ch.query(conn, "INSERT INTO helloworld.my_first_table (user_id, message, timestamp, metric) FORMAT RowBinary", rows)

Ch.query_rows(conn, "SELECT * FROM helloworld.my_first_table")
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

### Use cases

#### Custom FORMAT in SELECT

```elixir
iex> Ch.query(conn, "SELECT 1 + 1")
{:ok, "2\n"}

iex> Ch.query(conn, "SELECT 1 + 1 FORMAT RowBinary")
{:ok, <<2, 0>>}

iex> Ch.query(conn, "SELECT 1 + 1 FORMAT CSVWithNames")
{:ok, "\"plus(1, 1)\"\n2\n"}
```

#### SELECT with params

```elixir
# https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters
iex> statement = "SELECT {a:Array(UInt8)}, {b:UInt8}, {c:String}, {d:DateTime}"
iex> params = %{a: [1,2], b: 123, c: "123", d: NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)}
iex> Ch.query_rows(conn, statement, params)
{:ok, %{num_rows: 1, rows: [[[1, 2], 123, "123", ~N[2023-01-06 03:06:32]]]}}
```

#### INSERT a RowBinary stream

```elixir
iex> Ch.query(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) ENGINE=Memory")

iex> rows = [[1, "1", ~N[2022-11-26 09:38:24]], [2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]
iex> types = [:u32, :string, :datetime]
iex> stream_or_iodata = rows |> Stream.chunk_every(20) |> Stream.map(fn chunk -> Ch.encode_row_binary_chunk(chunk, types) end)
# `stream_or_iodata` is sent as a chunked request (~ Stream.each(stream_or_iodata, fn chunk -> send_chunk(chunk) end))
iex> Ch.query(conn, "INSERT INTO example(a, b, c) FORMAT RowBinary", stream_or_iodata)
{:ok, %{num_rows: 3, rows: []}}
```

#### INSERT a CSV file stream

```elixir
iex> File.write!("example.csv", "1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example(a, b, c) FORMAT CSV", File.stream!("example.csv"))
{:ok, %{num_rows: 3, rows: []}}
```

#### INSERT a CSVWithNames file stream

```elixir
iex> File.write!("example.csv", "a,b,c\n1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example FORMAT CSVWithNames", File.stream!("example.csv"))
{:ok, %{num_rows: 3, rows: []}}
```
