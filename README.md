HTTP-based ClickHouse client. Uses [`transfer-encoding: chunked`](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) for inserts.

Usage:

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123, database: "default")
{:ok, #PID<0.240.0>}

iex> Ch.query(conn, "SELECT 1 + 1")
{:ok, [[2]]}

# https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters
iex> Ch.query(conn, "SELECT 1 + {a:Int8}", %{a: 2})
{:ok, [[3]]}

iex> Ch.query(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) engine=Memory")
{:ok, []}

iex> Ch.query(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) engine=Memory")
{:error,
 %Ch.Error{
   message: "Code: 57. DB::Exception: Table default.example already exists. (TABLE_ALREADY_EXISTS) (version 22.10.1.1175 (official build))\n"
 }}

iex> Ch.query(conn, "SHOW TABLES")
{:ok, [["example"]]}

# inserts support Enumerable.t, meaning lists or streams
# by default the enumerable is encoded to a CSV stream and sent as a chunked request with chunk=row
iex> enumerable = [[1, "1", ~N[2022-11-26 09:38:24]], [2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]
iex> Ch.query(conn, "INSERT INTO example(a, b, c)", enumerable)
{:ok, _rows_written = 3}

iex> Ch.query(conn, "SELECT * FROM example WHERE a > {a:Int8}", %{a: 1})
{:ok, [[2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]}

iex> Ch.query(conn, "ALTER TABLE example DELETE WHERE a < {a:Int8}", %{a: 100})
{:ok, []}

iex> Ch.query(conn, "SELECT count() FROM example")
{:ok, [[0]]}

# %File.Stream{} is not encoded as is sent as is
iex> File.write!("example.csv", "1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example(a, b, c)", File.stream!("example.csv"), format: "CSV")
{:ok, _rows_written = 3}

iex> File.write!("example.csv", "a,b,c\n1,1,2022-11-26 09:38:24\n2,2,2022-11-26 09:38:25\n3,3,2022-11-26 09:38:26")
iex> Ch.query(conn, "INSERT INTO example", File.stream!("example.csv"), format: "CSVWithNames")
{:ok, _rows_written = 3}

iex> Ch.query(conn, "SELECT count() FROM {table:Identifier}", %{"table" => "example"})
{:ok, [[6]]}

iex> File.rm!("example.csv")
iex> Ch.query(conn, "DROP TABLE example")
{:ok, []}

iex> Ch.query(conn, "SHOW TABLES")
{:ok, []}
```
