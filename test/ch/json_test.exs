defmodule Ch.JSONTest do
  use ExUnit.Case, async: true

  @moduletag :json

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "select literal json", %{pool: pool} do
    select = fn literal ->
      assert %{rows: [[value]]} =
               Ch.query!(pool, "select '#{literal}'::json", _params = %{},
                 settings: %{"output_format_binary_write_json_as_string" => 1}
               )

      value
    end

    assert select.(~s|{}|) == %{}
    assert select.(~s|{"a":"b","c":"d"}|) == %{"a" => "b", "c" => "d"}
    assert select.(~s|{"a":42}|) == %{"a" => 42}
    assert select.(~s|{"a":3.14}|) == %{"a" => 3.14}
    assert select.(~s|{"a":true}|) == %{"a" => true}
    assert select.(~s|{"a":false}|) == %{"a" => false}
    assert select.(~s|{"a":{"b":"c"}}|) == %{"a" => %{"b" => "c"}}
    assert select.(~s|{"a":[1,2,3]}|) == %{"a" => [1, 2, 3]}
    assert select.(~s|{"a":[]}|) == %{"a" => []}
    assert select.(~s|{"a":[null]}|) == %{"a" => [nil]}
    assert select.(~s|{"a":[1,3.14,"hello",null]}|) == %{"a" => [1, 3.14, "hello", nil]}
    assert select.(~s|{"a":[1,2.13,"s",{"a":"b"}]}|) == %{"a" => [1, 2.13, "s", %{"a" => "b"}]}

    # now the weird bits:
    # - null fields are removed
    assert select.(~s|{"a":null}|) == %{}
    # - fields with dots are treated as nested objects
    assert select.(~s|{"a.b":"c"}|) == %{"a" => %{"b" => "c"}}
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "basic", %{pool: pool} do
    Help.query!("CREATE TABLE json_test (json JSON, id UInt8) ENGINE = Memory")
    on_exit(fn -> Help.query!("drop table json_test") end)

    Ch.query!(pool, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}', 0),
    ('{"f" : "Hello, World!"}', 1),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}', 2)
    """)

    assert Ch.query!(pool, "SELECT json FROM json_test ORDER BY id", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows == [
             [%{"a" => %{"b" => 42}, "c" => [1, 2, 3]}],
             [%{"f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43, "e" => 10}, "c" => [4, 5, 6]}]
           ]

    rows = [[%{"a" => %{"b" => 999}, "some other" => "json value", "from" => "rowbinary"}, 3]]
    rowbinary = Ch.RowBinary.encode_rows(rows, ["JSON", "UInt8"])

    Ch.query!(
      pool,
      ["INSERT INTO json_test(json, id) FORMAT RowBinary\n" | rowbinary],
      _params = %{},
      settings: %{"input_format_binary_read_json_as_string" => 1}
    )

    assert Ch.query!(
             pool,
             "SELECT json FROM json_test where json.from = 'rowbinary'",
             _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows ==
             [
               [%{"from" => "rowbinary", "some other" => "json value", "a" => %{"b" => 999}}]
             ]

    assert Ch.query!(
             pool,
             "select json.a.b, json.a.g, json.c, json.d from json_test order by id",
             _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows ==
             [
               [42, nil, [1, 2, 3], nil],
               [nil, nil, nil, nil],
               [43, nil, [4, 5, 6], nil],
               [999, nil, nil, nil]
             ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#using-json-in-a-table-column-definition
  test "with skip (i.e. extra type options)", %{pool: pool} do
    Help.query!("CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;")
    on_exit(fn -> Help.query!("drop table json_test") end)

    Ch.query!(pool, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'),
    ('{"f" : "Hello, World!"}'),
    ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
    """)

    assert Ch.query!(pool, "SELECT json FROM json_test", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows == [
             [%{"a" => %{"b" => 42}, "c" => [1, 2, 3]}],
             [%{"a" => %{"b" => 0}, "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43}, "c" => [4, 5, 6]}]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-paths-as-sub-columns
  test "reading json paths as subcolumns", %{pool: pool} do
    Help.query!("CREATE TABLE json_test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory")
    on_exit(fn -> Help.query!("drop table json_test") end)

    Ch.query!(pool, """
    INSERT INTO json_test VALUES
    ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'),
    ('{"f" : "Hello, World!", "d" : "2020-01-02"}'),
    ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
    """)

    assert Ch.query!(pool, "SELECT json FROM json_test", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows == [
             [%{"a" => %{"b" => 42, "g" => 42.42}, "c" => [1, 2, 3], "d" => "2020-01-01"}],
             [%{"a" => %{"b" => 0}, "d" => "2020-01-02", "f" => "Hello, World!"}],
             [%{"a" => %{"b" => 43, "g" => 43.43}, "c" => [4, 5, 6]}]
           ]

    assert Ch.query!(
             pool,
             "SELECT json.a.b, json.a.g, json.c, json.d FROM json_test",
             _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows ==
             [
               [42, 42.42, [1, 2, 3], ~D[2020-01-01]],
               [0, nil, nil, ~D[2020-01-02]],
               [43, 43.43, [4, 5, 6], nil]
             ]

    assert Ch.query!(pool, "SELECT json.non.existing.path FROM json_test", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows ==
             [
               [nil],
               [nil],
               [nil]
             ]

    assert Ch.query!(
             pool,
             "SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM json_test;"
           ).rows ==
             [
               ["UInt32", "Dynamic", "Dynamic", "Dynamic"],
               ["UInt32", "Dynamic", "Dynamic", "Dynamic"],
               ["UInt32", "Dynamic", "Dynamic", "Dynamic"]
             ]

    assert Ch.query!(pool, """
           SELECT
             json.a.g.:Float64,
             dynamicType(json.a.g),
             json.d.:Date,
             dynamicType(json.d)
           FROM json_test
           """).rows == [
             [42.42, "Float64", ~D[2020-01-01], "Date"],
             [nil, "None", ~D[2020-01-02], "Date"],
             [43.43, "Float64", nil, "None"]
           ]

    assert Ch.query!(pool, "SELECT json.a.g::UInt64 AS uint FROM json_test").rows == [
             [42],
             [0],
             [43]
           ]

    assert_raise Ch.Error, ~r/Conversion between numeric types and UUID is not supported/, fn ->
      Ch.query!(pool, "SELECT json.a.g::UUID AS float FROM json_test;")
    end
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#reading-json-sub-objects-as-sub-columns
  test "reading json subobjects as subcolumns", %{pool: pool} do
    Help.query!("CREATE TABLE json_test (json JSON) ENGINE = Memory;")
    on_exit(fn -> Help.query!("drop table json_test") end)

    Ch.query!(
      pool,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'),
      ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'),
      ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
      """
    )

    assert Ch.query!(pool, "SELECT json FROM json_test", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows == [
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

    assert Ch.query!(pool, "SELECT json.^a.b, json.^d.e.f FROM json_test;", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows ==
             [
               [%{"c" => 42, "g" => 42.42}, %{"g" => "Hello, World", "h" => [1, 2, 3]}],
               [%{}, %{"h" => [4, 5, 6]}],
               [%{"c" => 43, "e" => 10, "g" => 43.43}, %{}]
             ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/newjson#handling-arrays-of-json-objects
  test "handling arrays of json objects", %{pool: pool} do
    Help.query!("CREATE TABLE json_test (json JSON) ENGINE = Memory;")
    on_exit(fn -> Help.query!("drop table json_test") end)

    Ch.query!(
      pool,
      """
      INSERT INTO json_test VALUES
      ('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
      ('{"a" : {"b" : [1, 2, 3]}}'),
      ('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
      """
    )

    assert Ch.query!(pool, "SELECT json FROM json_test;", _params = %{},
             settings: %{"output_format_binary_write_json_as_string" => 1}
           ).rows == [
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

    # TODO
    assert_raise ArgumentError, "unsupported dynamic type JSON", fn ->
      Ch.query!(pool, "SELECT json.a.b, dynamicType(json.a.b) FROM json_test;")
    end

    assert_raise ArgumentError, "unsupported dynamic type JSON", fn ->
      Ch.query!(
        pool,
        "SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM json_test;"
      )
    end
  end
end
