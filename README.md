HTTP-based ClickHouse client.

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123, database: "default")
{:ok, pid}

iex> Ch.query(conn, "SELECT 1 + 1")
{:ok, }

iex> Ch.query(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) engine=Memory")

iex> Ch.query(conn, "SHOW TABLES")

iex> to_insert = Enum.map(1..3, fn i -> [i, to_string(i), NaiveDateTime.add(NaiveDateTime.utc_now(), i)] end)
[
  [1, "1", ~N[2022-11-26 09:38:24.986596]],
  [2, "2", ~N[2022-11-26 09:38:25.989127]],
  [3, "3", ~N[2022-11-26 09:38:26.989131]]
]

iex> Ch.query(conn, "INSERT INTO example(a, b, c)", to_insert)

iex> Ch.query(conn, "SELECT * FROM example WHERE a > {a:Int8}", %{a: 1})

iex> Ch.query(conn, "ALTER TABLE example DELETE WHERE a < {a:Int8}", %{a: 100})

iex> Ch.query(conn, "DROP TABLE example")
```
