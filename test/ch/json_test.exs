defmodule Ch.JSONTest do
  use ExUnit.Case

  @moduletag :json

  setup do
    on_exit(fn ->
      Ch.Test.query("DROP TABLE IF EXISTS json_test", [], database: Ch.Test.database())
    end)

    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "simple json", %{conn: conn} do
    select = fn literal ->
      [[value]] = Ch.query!(conn, "select '#{literal}'::json").rows
      value
    end

    assert select.(~s|{"a":"b","c":"d"}|) == %{"a" => "b", "c" => "d"}

    # note that 42 is a string here, not an integer
    assert select.(~s|{"a":42}|) == %{"a" => "42"}

    assert select.(~s|{}|) == %{}

    # null fields are removed?
    assert select.(~s|{"a":null}|) == %{}

    assert select.(~s|{"a":3.14}|) == %{"a" => 3.14}

    assert select.(~s|{"a":true}|) == %{"a" => true}

    assert select.(~s|{"a":false}|) == %{"a" => false}

    assert select.(~s|{"a":{"b":"c"}}|) == %{"a" => %{"b" => "c"}}

    # numbers in arrays become strings
    assert select.(~s|{"a":[1,2,3]}|) == %{"a" => ["1", "2", "3"]}

    # this is weird, fields with dots are treated as nested objects
    assert select.(~s|{"a.b":"c"}|) == %{"a" => %{"b" => "c"}}

    assert select.(~s|{"a":[]}|) == %{"a" => []}

    assert select.(~s|{"a":[null]}|) == %{"a" => [nil]}

    # everything in an array gets converted to "lcd" type, aka string
    assert select.(~s|{"a":[1,3.14,"hello",null]}|) == %{"a" => ["1", "3.14", "hello", nil]}

    # but not if the array has nested objects, then the array becomes a tuple and can support mixed types
    assert select.(~s|{"a":[1,2.13,"s",{"a":"b"}]}|) == %{"a" => ["1", 2.13, "s", %{"a" => "b"}]}
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "basic", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE json_test (json JSON) ENGINE = Memory")

    Ch.query!(conn, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}')
    """)

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test"
           ).rows == [
             [%{"a" => %{"b" => "42"}, "c" => ["1", "2", "3"]}],
             [%{"f" => "Hello, World!"}],
             [%{"a" => %{"b" => "43", "e" => "10"}, "c" => ["4", "5", "6"]}]
           ]

    Ch.query!(
      conn,
      "INSERT INTO json_test FORMAT RowBinary",
      [[%{"some other" => "json value", "from" => "rowbinary"}]],
      types: ["JSON"]
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test where json.from = 'rowbinary'"
           ).rows == [
             [%{"from" => "rowbinary", "some other" => "json value"}]
           ]

    assert Ch.query!(conn, "select json.a.b, json.a.g, json.c, json.d from json_test").rows == [
             [42, nil, [1, 2, 3], nil],
             [nil, nil, nil, nil],
             [43, nil, [4, 5, 6], nil],
             [nil, nil, nil, nil]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "with skip (i.e. extra type options)", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;")

    Ch.query!(conn, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
    """)

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test"
           ).rows == [
             [%{"a" => %{"b" => 42}, "c" => ["1", "2", "3"]}],
             [%{"a" => %{"b" => 0}, "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43}, "c" => ["4", "5", "6"]}]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-paths-as-sub-columns
  test "reading json paths as subcolumns", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory")

    Ch.query!(conn, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'),
    ('{"f" : "Hello, World!", "d" : "2020-01-02"}'),
    ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
    """)

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test"
           ).rows == [
             [%{"a" => %{"b" => 42, "g" => 42.42}, "c" => ["1", "2", "3"], "d" => "2020-01-01"}],
             [%{"a" => %{"b" => 0}, "d" => "2020-01-02", "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43, "g" => 43.43}, "c" => ["4", "5", "6"]}]
           ]

    assert Ch.query!(conn, "SELECT json.a.b, json.a.g, json.c, json.d FROM json_test").rows == [
             [42, 42.42, [1, 2, 3], ~D[2020-01-01]],
             [0, nil, nil, ~D[2020-01-02]],
             [43, 43.43, [4, 5, 6], nil]
           ]
  end
end
