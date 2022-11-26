HTTP-based ClickHouse client.

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123, database: "default")

iex> Ch.query(conn, "SELECT 1 + 1")

iex> Ch.query(conn, "CREATE TABLE demo ")

iex> Ch.query(conn, "SHOW TABLES")

iex> Ch.query(conn, "INSERT INTO demo(a)", [[1, 2, 3], [4, 5, 6]])

iex> Ch.query(conn, "INSERT INTO demo(a) VALUES(?, ?, ?)", [1, 2, 3])

iex> Ch.query(conn, "SELECT * FROM demo WHERE id > ?", [1])

iex> Ch.query(conn, "SELECT * FROM demo WHERE id > $1", [1])

iex> Ch.query(conn, "SELECT * FROM demo WHERE id > @id", [id: 1])

iex> Ch.query(conn, "ALTER TABLE demo DELETE WHERE id < ?", [1])

iex> Ch.query(conn, "ALTER TABLE demo DELETE WHERE id > $1", [8])
```
