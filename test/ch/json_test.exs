defmodule Ch.JSONTest do
  use ExUnit.Case, async: true

  @moduletag :json

  @table "json_test"

  setup do
    conn = start_supervised!({Ch, database: Ch.Test.database(), settings: [enable_json_type: 1]})

    on_exit(fn ->
      Ch.Test.query("DROP TABLE IF EXISTS #{@table}", [], database: Ch.Test.database())
    end)

    {:ok, conn: conn}
  end

  test "simple json", %{conn: conn} do
    assert Ch.query!(conn, ~s|select '{"a":"b","c":"d"}'::json|).rows == [
             [%{"a" => "b", "c" => "d"}]
           ]
  end

  # TODO
  @tag :skip
  test "creating json", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE #{@table} (json JSON) ENGINE = Memory")

    Ch.query!(conn, """
    INSERT INTO #{@table} VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}')
    """)

    assert Ch.query!(
             conn,
             "SELECT json FROM #{@table}"
           ).rows == [
             [%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"]}],
             [%{"f" => "Hello, World!"}],
             [%{"a" => %{"b" => "43", "e" => "10"}, "c" => ["4", "5", "6"]}]
           ]
  end
end
