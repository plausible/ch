HTTP-based ClickHouse client.

```elixir
iex> Mix.install([{:ch, github: "ruslandoga/ch"}])

iex> {:ok, conn} = Ch.start_link(scheme: "http", host: "localhost", port: 8123, database: "default")
{:ok, #PID<0.240.0>}

iex> Ch.query(conn, "SELECT 1 + 1")
{:ok, [[2]]}

iex> Ch.query(conn, "SELECT 1 + {a:Int8}", %{a: 2})
{:ok, [[3]]}

iex> Ch.query(conn, "CREATE TABLE example(a UInt32, b String, c DateTime) engine=Memory")
{:ok, []}

iex> Ch.query(conn, "SHOW TABLES")
{:ok,
 [
   [".inner_id.a4735445-0110-498c-851c-1ee93453a644"],
   ["example"],
   ["material_view"]
 ]}

iex> to_insert = [
...>   [1, "1", ~N[2022-11-26 09:38:24]],
...>   [2, "2", ~N[2022-11-26 09:38:25]],
...>   [3, "3", ~N[2022-11-26 09:38:26]]
...> ]

iex> Ch.query(conn, "INSERT INTO example(a, b, c)", to_insert)
{:ok, []}

iex> Ch.query(conn, "SELECT * FROM example WHERE a > {a:Int8}", %{a: 1})
{:ok, [[2, "2", ~N[2022-11-26 09:38:25]], [3, "3", ~N[2022-11-26 09:38:26]]]}

iex> Ch.query(conn, "ALTER TABLE example DELETE WHERE a < {a:Int8}", %{a: 100})
{:ok, []}

iex> Ch.query(conn, "SELECT count() FROM example")
{:ok, [[0]]}

iex> Ch.query(conn, "DROP TABLE example")
{:ok, []}

iex> Ch.query(conn, "SHOW TABLES")
{:ok,
 [
   [".inner_id.a4735445-0110-498c-851c-1ee93453a644"],
   ["material_view"]
 ]}
```
