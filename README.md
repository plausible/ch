Minimal HTTP ClickHouse client.

Usage:

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123, database: "default")
{:ok, #PID<0.240.0>}

# SELECT
iex> Ch.query(conn, "SELECT 1 + 1")
{:ok, "2\n"}

iex> Ch.query(conn, "SELECT 1 + 1 FORMAT RowBinary")
{:ok, <<2, 0>>}

iex> Ch.query(conn, "SELECT 1 + 1 FORMAT CSVWithNames")
{:ok, "\"plus(1, 1)\"\n2\n"}

iex> with {:ok, data} <- Ch.query(conn, "SELECT 1 + 1 FORMAT RowBinaryWithNamesAndTypes"), do: Ch.decode_rows(data)
[[2]]

iex> conn |> Ch.query!("SELECT 1 + {$0:Int8} FORMAT RowBinaryWithNamesAndTypes", _params = [2]) |> Ch.decode_rows()
[[3]]

# `query_rows` is a helper that uses `FORMAT RowBinaryWithNamesAndTypes` and decodes the response automatically
iex> Ch.query_rows(conn, "SELECT 1 + 1")
{:ok, %{num_rows: 1, rows: [[2]]}}

# https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters
iex> statement = "SELECT {a:Array(UInt8)}, {b:UInt8}, {c:String}, {d:DateTime}"
iex> params = %{a: [1,2], b: 123, c: "123", d: NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)}
iex> Ch.query_rows(conn, statement, params)
{:ok, %{num_rows: 1, rows: [[[1, 2], 123, "123", ~N[2023-01-06 03:06:32]]]}}

# CREATE
iex> Ch.query_rows(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) ENGINE=Memory")
{:ok, %{num_rows: 0, rows: []}}

iex> Ch.query_rows(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) ENGINE=Memory")
{:error, %Ch.Error{message: "Code: 57. DB::Exception: Table default.example already exists. (TABLE_ALREADY_EXISTS) (version 22.10.1.1175 (official build))\n"}}

iex> Ch.query_rows(conn, "SHOW TABLES")
{:ok, %{num_rows: 1, rows: [["example"]]}}

# INSERT
iex> rows = [[1, "1", ~N[2022-11-26 09:38:24]], [2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]
iex> types = [:u32, :string, :datetime]
iex> stream_or_iodata = Stream.map(rows, fn row -> Ch.encode_row(row, types) end)
# `stream_or_iodata` is sent as a chunked request (~ Stream.each(stream_or_iodata, fn chunk -> send_chunk(chunk) end))
iex> Ch.query(conn, "INSERT INTO example(a, b, c) FORMAT RowBinary", stream_or_iodata)
{:ok, %{num_rows: 3, rows: []}}

# for `SELECT` queries `RowBinaryWithNamesAndTypes` format is used (~ "SELECT * FROM example WHERE a > {a:Int8} FORMAT RowBinaryWithNamesAndTypes")
iex> Ch.query_rows(conn, "SELECT * FROM example WHERE a > {a:Int8}", %{a: 1})
{:ok, %{num_rows: 2, rows: [[2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]}}

iex> Ch.query_rows(conn, "ALTER TABLE example DELETE WHERE a < {a:Int8}", %{a: 100})
{:ok, %{num_rows: 0, rows: []}}

iex> Ch.query_rows(conn, "SELECT count() FROM example")
{:ok, %{num_rows: 1, rows: [[0]]}}

iex> File.write!("example.csv", "1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example(a, b, c) FORMAT CSV", File.stream!("example.csv"))
{:ok, %{num_rows: 3, rows: []}}

iex> File.write!("example.csv", "a,b,c\n1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example FORMAT CSVWithNames", File.stream!("example.csv"))
{:ok, %{num_rows: 3, rows: []}}

iex> Ch.query_rows(conn, "SELECT count() FROM {table:Identifier}", %{"table" => "example"})
{:ok, %{num_rows: 1, rows: [[6]]}}

iex> File.rm!("example.csv")
iex> Ch.query(conn, "DROP TABLE example")
{:ok, ""}

iex> Ch.query_rows(conn, "SHOW TABLES")
{:ok, %{num_rows: 0, rows: []}}
```
