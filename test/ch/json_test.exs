defmodule Ch.JSONTest do
  use ExUnit.Case

  setup do
    conn =
      start_supervised!(
        {Ch,
         database: Ch.Test.database(),
         settings: [
           enable_json_type: 1,
           input_format_binary_read_json_as_string: 1,
           output_format_binary_write_json_as_string: 1
         ]}
      )

    {:ok, conn: conn}
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/newjson#creating-json
  test "Creating JSON", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (json JSON) ENGINE = Memory")
    on_exit(fn -> Ch.Test.sql_exec("DROP TABLE test") end)

    Ch.query!(conn, """
    INSERT INTO test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}')
    """)

    assert Ch.query!(conn, "SELECT json FROM test").rows == [
             [%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"]}],
             [%{"f" => "Hello, World!"}],
             [%{"a" => %{"b" => "43", "e" => "10"}, "c" => ["4", "5", "6"]}]
           ]
  end

  @tag :skip
  test "Creating JSON (explicit types and SKIP)", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory")
    on_exit(fn -> Ch.Test.sql_exec("DROP TABLE test") end)

    Ch.query!(conn, """
    INSERT INTO test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}')
    """)

    assert Ch.query!(conn, "SELECT json FROM test").rows == []
  end

  test "Creating JSON using CAST from String", %{conn: conn} do
    assert Ch.query!(conn, """
           SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json
           """).rows == [
             [%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"], "d" => "Hello, World!"}]
           ]
  end

  test "Creating JSON using CAST from Tuple", %{conn: conn} do
    assert Ch.query!(
             conn,
             """
             SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json
             """,
             [],
             settings: [enable_named_columns_in_function_tuple: 1]
           ).rows == [[%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"], "d" => "Hello, World!"}]]
  end

  test "Creating JSON using CAST from Map", %{conn: conn} do
    assert Ch.query!(
             conn,
             """
             SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
             """,
             [],
             settings: [enable_variant_type: 1, use_variant_as_common_type: 1]
           ).rows == [[%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"], "d" => "Hello, World!"}]]
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/newjson#reading-json-paths-as-subcolumns
  test "Reading JSON paths as subcolumns", %{conn: _conn} do
  end
end
