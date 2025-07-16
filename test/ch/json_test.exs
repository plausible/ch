defmodule Ch.JSONTest do
  use ExUnit.Case, async: true

  @moduletag :json

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
end
