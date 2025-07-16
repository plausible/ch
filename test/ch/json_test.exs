defmodule Ch.JSONTest do
  use ExUnit.Case, async: true

  @moduletag :json

  @table "json_test"

  setup do
    conn =
      start_supervised!(
        {Ch,
         database: Ch.Test.database(),
         settings: [
           enable_json_type: 1,
           output_format_binary_write_json_as_string: 1,
           input_format_binary_read_json_as_string: 1
         ]}
      )

    on_exit(fn ->
      Ch.Test.query("DROP TABLE IF EXISTS #{@table}", [], database: Ch.Test.database())
    end)

    {:ok, conn: conn}
  end

  test "simple json", %{conn: conn} do
    assert Ch.query!(conn, ~s|select '{"a":"b","c":"d"}'::json|).rows == [
             [%{"a" => "b", "c" => "d"}]
           ]

    assert Ch.query!(conn, ~s|select '{"a":42}'::json|).rows == [[%{"a" => "42"}]]
    assert Ch.query!(conn, ~s|select '{}'::json|).rows == [[%{}]]
    assert Ch.query!(conn, ~s|select '{"a":null}'::json|).rows == [[%{}]]
    assert Ch.query!(conn, ~s|select '{"a":3.14}'::json|).rows == [[%{"a" => 3.14}]]
    assert Ch.query!(conn, ~s|select '{"a":true}'::json|).rows == [[%{"a" => true}]]
    assert Ch.query!(conn, ~s|select '{"a":false}'::json|).rows == [[%{"a" => false}]]
    assert Ch.query!(conn, ~s|select '{"a":{"b":"c"}}'::json|).rows == [[%{"a" => %{"b" => "c"}}]]

    assert Ch.query!(conn, ~s|select '{"a":[]}'::json|).rows == [
             [%{"a" => []}]
           ]

    assert Ch.query!(conn, ~s|select '{"a":[null]}'::json|).rows == [
             [%{"a" => [nil]}]
           ]

    assert Ch.query!(conn, ~s|select '{"a":[1,3.14,"hello",null]}'::json|).rows == [
             [%{"a" => ["1", "3.14", "hello", nil]}]
           ]

    assert Ch.query!(conn, ~s|select '{"a":[1,2.13,"s",{"a":"b"}]}'::json|).rows == [
             [%{"a" => ["1", 2.13, "s", %{"a" => "b"}]}]
           ]
  end

  test "creating, inserting, and reading json", %{conn: conn} do
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

    Ch.query!(
      conn,
      "INSERT INTO #{@table} FORMAT RowBinary",
      [[%{"some other" => "json value", "from" => "rowbinary"}]],
      types: ["JSON"]
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM #{@table} where json.from = 'rowbinary'"
           ).rows == [
             [%{"from" => "rowbinary", "some other" => "json value"}]
           ]
  end
end
