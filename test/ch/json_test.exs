defmodule Ch.JSONTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  @moduletag :json

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  setup do
    on_exit(fn -> Ch.Test.query("DROP TABLE IF EXISTS json_test") end)
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "simple json", %{conn: conn, query_options: query_options} do
    select = fn literal ->
      [[value]] = Ch.query!(conn, "select '#{literal}'::json", [], query_options).rows
      value
    end

    assert select.(~s|{"a":"b","c":"d"}|) == %{"a" => "b", "c" => "d"}

    # note that 42 was a string in pre-25.0 and post-25.8 ClickHouse versions

    assert select.(~s|{"a":42}|) == %{"a" => 42}

    assert select.(~s|{}|) == %{}

    # null fields are removed?
    assert select.(~s|{"a":null}|) == %{}

    assert select.(~s|{"a":3.14}|) == %{"a" => 3.14}

    assert select.(~s|{"a":true}|) == %{"a" => true}

    assert select.(~s|{"a":false}|) == %{"a" => false}

    assert select.(~s|{"a":{"b":"c"}}|) == %{"a" => %{"b" => "c"}}

    # numbers in arrays become strings
    assert select.(~s|{"a":[1,2,3]}|) == %{"a" => [1, 2, 3]}

    # this is weird, fields with dots are treated as nested objects
    assert select.(~s|{"a.b":"c"}|) == %{"a" => %{"b" => "c"}}

    assert select.(~s|{"a":[]}|) == %{"a" => []}

    assert select.(~s|{"a":[null]}|) == %{"a" => [nil]}

    # everything in an array gets converted to "lcd" type, aka string
    assert select.(~s|{"a":[1,3.14,"hello",null]}|) == %{"a" => [1, 3.14, "hello", nil]}

    # but not if the array has nested objects, then the array becomes a tuple and can support mixed types
    assert select.(~s|{"a":[1,2.13,"s",{"a":"b"}]}|) == %{"a" => [1, 2.13, "s", %{"a" => "b"}]}
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "basic", %{conn: conn, query_options: query_options} do
    Ch.query!(
      conn,
      "CREATE TABLE json_test (json JSON, id UInt8) ENGINE = Memory",
      [],
      query_options
    )

    Ch.query!(
      conn,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}', 0),
      ('{"f" : "Hello, World!"}', 1),
      ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}', 2)
      """,
      [],
      query_options
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test ORDER BY id",
             [],
             query_options
           ).rows == [
             [%{"a" => %{"b" => 42}, "c" => [1, 2, 3]}],
             [%{"f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43, "e" => 10}, "c" => [4, 5, 6]}]
           ]

    Ch.query!(
      conn,
      "INSERT INTO json_test(json, id) FORMAT RowBinary",
      [[%{"a" => %{"b" => 999}, "some other" => "json value", "from" => "rowbinary"}, 3]],
      Keyword.merge(query_options, types: ["JSON", "UInt8"])
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test where json.from = 'rowbinary'",
             [],
             query_options
           ).rows == [
             [%{"from" => "rowbinary", "some other" => "json value", "a" => %{"b" => 999}}]
           ]

    assert Ch.query!(
             conn,
             "select json.a.b, json.a.g, json.c, json.d from json_test order by id",
             [],
             query_options
           ).rows ==
             [
               [42, nil, [1, 2, 3], nil],
               [nil, nil, nil, nil],
               [43, nil, [4, 5, 6], nil],
               [999, nil, nil, nil]
             ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "with skip (i.e. extra type options)", %{conn: conn, query_options: query_options} do
    Ch.query!(
      conn,
      "CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;",
      [],
      query_options
    )

    Ch.query!(
      conn,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
      ('{"f" : "Hello, World!"}'),
      ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
      """,
      [],
      query_options
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test",
             [],
             query_options
           ).rows == [
             [%{"a" => %{"b" => 42}, "c" => [1, 2, 3]}],
             [%{"a" => %{"b" => 0}, "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43}, "c" => [4, 5, 6]}]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-paths-as-sub-columns
  test "reading json paths as subcolumns", %{conn: conn, query_options: query_options} do
    Ch.query!(
      conn,
      "CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory",
      [],
      query_options
    )

    Ch.query!(
      conn,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'),
      ('{"f" : "Hello, World!", "d" : "2020-01-02"}'),
      ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
      """,
      [],
      query_options
    )

    assert Ch.query!(
             conn,
             "SELECT json FROM json_test",
             [],
             query_options
           ).rows == [
             [%{"a" => %{"b" => 42, "g" => 42.42}, "c" => [1, 2, 3], "d" => "2020-01-01"}],
             [%{"a" => %{"b" => 0}, "d" => "2020-01-02", "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43, "g" => 43.43}, "c" => [4, 5, 6]}]
           ]

    assert Ch.query!(
             conn,
             "SELECT json.a.b, json.a.g, json.c, json.d FROM json_test",
             [],
             query_options
           ).rows == [
             [42, 42.42, [1, 2, 3], ~D[2020-01-01]],
             [0, nil, nil, ~D[2020-01-02]],
             [43, 43.43, [4, 5, 6], nil]
           ]

    assert Ch.query!(conn, "SELECT json.non.existing.path FROM json_test", [], query_options).rows ==
             [
               [nil],
               [nil],
               [nil]
             ]

    assert Ch.query!(
             conn,
             "SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM json_test;",
             [],
             query_options
           ).rows == [
             ["UInt32", "Dynamic", "Dynamic", "Dynamic"],
             ["UInt32", "Dynamic", "Dynamic", "Dynamic"],
             ["UInt32", "Dynamic", "Dynamic", "Dynamic"]
           ]

    assert Ch.query!(
             conn,
             """
             SELECT
               json.a.g.:Float64,
               dynamicType(json.a.g),
               json.d.:Date,
               dynamicType(json.d)
             FROM json_test
             """,
             [],
             query_options
           ).rows == [
             [42.42, "Float64", ~D[2020-01-01], "Date"],
             [nil, "None", ~D[2020-01-02], "Date"],
             [43.43, "Float64", nil, "None"]
           ]

    assert Ch.query!(
             conn,
             """
             SELECT json.a.g::UInt64 AS uint
             FROM json_test;
             """,
             [],
             query_options
           ).rows == [
             [42],
             [0],
             [43]
           ]

    assert_raise Ch.Error, ~r/Conversion between numeric types and UUID is not supported/, fn ->
      Ch.query!(conn, "SELECT json.a.g::UUID AS float FROM json_test;", [], query_options)
    end
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-sub-objects-as-sub-columns
  test "reading json subobjects as subcolumns", %{conn: conn, query_options: query_options} do
    Ch.query!(conn, "CREATE TABLE json_test (json JSON) ENGINE = Memory;", [], query_options)

    Ch.query!(
      conn,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'),
      ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'),
      ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
      """,
      [],
      query_options
    )

    assert Ch.query!(conn, "SELECT json FROM json_test;", [], query_options).rows == [
             [
               %{
                 "a" => %{"b" => %{"c" => 42, "g" => 42.42}},
                 "c" => [1, 2, 3],
                 "d" => %{"e" => %{"f" => %{"g" => "Hello, World", "h" => [1, 2, 3]}}}
               }
             ],
             [%{"d" => %{"e" => %{"f" => %{"h" => [4, 5, 6]}}}, "f" => "Hello, World!"}],
             [
               %{
                 "a" => %{"b" => %{"c" => 43, "e" => 10, "g" => 43.43}},
                 "c" => [4, 5, 6]
               }
             ]
           ]

    assert Ch.query!(conn, "SELECT json.^a.b, json.^d.e.f FROM json_test;", [], query_options).rows ==
             [
               [%{"c" => 42, "g" => 42.42}, %{"g" => "Hello, World", "h" => [1, 2, 3]}],
               [%{}, %{"h" => [4, 5, 6]}],
               [%{"c" => 43, "e" => 10, "g" => 43.43}, %{}]
             ]
  end

  # TODO
  # https://clickhouse.com/docs/sql-reference/data-types/newjson#handling-arrays-of-json-objects
  test "handling arrays of json objects", %{conn: conn, query_options: query_options} do
    Ch.query!(conn, "CREATE TABLE json_test (json JSON) ENGINE = Memory;", [], query_options)

    Ch.query!(
      conn,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
      ('{"a" : {"b" : [1, 2, 3]}}'),
      ('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
      """,
      [],
      query_options
    )

    assert Ch.query!(conn, "SELECT json FROM json_test;", [], query_options).rows == [
             [
               %{
                 "a" => %{
                   "b" => [
                     %{
                       "c" => 42,
                       "d" => "Hello",
                       "f" => [[%{"g" => 42.42}]],
                       "k" => %{"j" => 1000}
                     },
                     %{"c" => 43},
                     %{
                       "d" => "My",
                       "e" => [1, 2, 3],
                       "f" => [[%{"g" => 43.43, "h" => "2020-01-01"}]],
                       "k" => %{"j" => 2000}
                     }
                   ]
                 }
               }
             ],
             [%{"a" => %{"b" => [1, 2, 3]}}],
             [
               %{
                 "a" => %{
                   "b" => [
                     %{"c" => 44, "f" => [[%{"h" => "2020-01-02"}]]},
                     %{
                       "d" => "World",
                       "e" => [4, 5, 6],
                       "f" => [[%{"g" => 44.44}]],
                       "k" => %{"j" => 3000}
                     }
                   ]
                 }
               }
             ]
           ]

    assert Ch.query!(conn, "SELECT json.a.b FROM json_test;", [], query_options).rows == [
             [
               [
                 %{"c" => 42, "d" => "Hello", "f" => [[%{"g" => 42.42}]], "k" => %{"j" => 1000}},
                 %{"c" => 43},
                 %{
                   "d" => "My",
                   "e" => [1, 2, 3],
                   "f" => [[%{"g" => 43.43, "h" => "2020-01-01"}]],
                   "k" => %{"j" => 2000}
                 }
               ]
             ],
             [[1, 2, 3]],
             [
               [
                 %{"c" => 44, "f" => [[%{"h" => "2020-01-02"}]]},
                 %{
                   "d" => "World",
                   "e" => [4, 5, 6],
                   "f" => [[%{"g" => 44.44}]],
                   "k" => %{"j" => 3000}
                 }
               ]
             ]
           ]

    assert Ch.query!(
             conn,
             "SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM json_test;",
             [],
             query_options
           ).rows == [
             [
               [42, 43, nil],
               [[[%{"g" => 42.42}]], nil, [[%{"g" => 43.43, "h" => "2020-01-01"}]]],
               ["Hello", nil, "My"]
             ],
             [[], [], []],
             [[44, nil], [[[%{"h" => "2020-01-02"}]], [[%{"g" => 44.44}]]], [nil, "World"]]
           ]

    assert_raise ArgumentError,
                 "Unsupported type definition (starting with 0x34) while decoding dynamic JSON. Only single-byte type identifiers are currently supported.",
                 fn ->
                   Ch.query!(
                     conn,
                     ~s|SELECT '{"a": "10:00:00.050"}'::JSON(a Time64)::Dynamic;|,
                     [],
                     query_options
                   )
                 end
  end
end
