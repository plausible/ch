defmodule Ch.DynamicTest do
  use ExUnit.Case

  @moduletag :dynamic

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "it works", %{conn: conn} do
    select = fn literal ->
      [row] = Ch.query!(conn, "select #{literal}::Dynamic as d, dynamicType(d)").rows
      row
    end

    Ch.query!(conn, "CREATE TABLE test (d Dynamic, id String) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)

    insert = fn value ->
      id = inspect(value)

      Ch.query!(conn, "insert into test(d, id) format RowBinary", [[value, id]],
        types: ["Dynamic", "String"]
      ).rows

      [[inserted]] =
        Ch.query!(conn, "select d from test where id = {id:String}", %{"id" => id}).rows

      inserted
    end

    # https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding

    # Nothing 0x00
    assert select.("[]::Array(Nothing)") == [[], "Array(Nothing)"]
    # assert insert.([]) == []

    # UInt8 0x01
    assert select.("0::UInt8") == [0, "UInt8"]
    assert select.("255::UInt8") == [255, "UInt8"]

    # UInt16 0x02
    assert select.("12::UInt16") == [12, "UInt16"]

    # UInt32 0x03
    assert select.("123::UInt32") == [123, "UInt32"]

    # UInt64 0x04
    assert select.("1234::UInt64") == [1234, "UInt64"]

    assert insert.(0) == 0
    assert insert.(255) == 255

    # UInt128 0x05
    assert select.("12345::UInt128") == [12345, "UInt128"]

    # UInt256 0x06
    assert select.("123456::UInt256") == [123_456, "UInt256"]

    # Int8 0x07
    assert select.("0::Int8") == [0, "Int8"]
    assert select.("-23::Int8") == [-23, "Int8"]

    # Int16 0x08
    assert select.("-12::Int16") == [-12, "Int16"]

    # Int32 0x09
    assert select.("123::Int32") == [123, "Int32"]

    # Int64 0x0A
    assert select.("-1234::Int64") == [-1234, "Int64"]

    assert insert.(-1234) == -1234

    # Int128 0x0B
    assert select.("12345::Int128") == [12345, "Int128"]

    # Int256 0x0C
    assert select.("-123456::Int256") == [-123_456, "Int256"]

    # Float32 0x0D
    assert select.("3.14::Float32") == [3.140000104904175, "Float32"]

    # Float64 0x0E
    assert select.("-3.14159::Float64") == [-3.14159, "Float64"]

    assert insert.(-3.14159) == -3.14159

    # Date 0x0F
    assert select.("'2020-01-01'::Date") == [~D[2020-01-01], "Date"]

    assert insert.(~D[2020-01-01]) == ~D[2020-01-01]

    # Date32 0x10
    assert select.("'2020-01-01'::Date32") == [~D[2020-01-01], "Date32"]

    # DateTime 0x11
    assert select.("'2020-01-01 12:34:56'::DateTime") == [
             Ch.Test.to_clickhouse_naive(conn, ~N[2020-01-01 12:34:56]),
             "DateTime"
           ]

    assert insert.(~N[2020-01-01 12:34:56]) ==
             Ch.Test.to_clickhouse_naive(conn, ~N[2020-01-01 12:34:56])

    # DateTime(time_zone) 0x12<var_uint_time_zone_name_size><time_zone_name_data>
    assert [dt, "DateTime('Europe/Prague')"] =
             select.("'2020-01-01 12:34:56'::DateTime('Europe/Prague')")

    assert inspect(dt) == "#DateTime<2020-01-01 12:34:56+01:00 CET Europe/Prague>"

    # DateTime64(P) 0x13<uint8_precision>
    assert select.("'2020-01-01 12:34:56.123456'::DateTime64(6)") ==
             [Ch.Test.to_clickhouse_naive(conn, ~N[2020-01-01 12:34:56.123456]), "DateTime64(6)"]

    # DateTime64(P, time_zone) 0x14<uint8_precision><var_uint_time_zone_name_size><time_zone_name_data>
    assert [dt64, "DateTime64(6, 'Europe/Prague')"] =
             select.("'2020-01-01 12:34:56.123456'::DateTime64(6, 'Europe/Prague')")

    assert inspect(dt64) == "#DateTime<2020-01-01 12:34:56.123456+01:00 CET Europe/Prague>"

    # String 0x15
    assert select.("'Hello, World!'") == ["Hello, World!", "String"]
    assert select.("0") == ["0", "String"]

    assert insert.("Hello, World!") == "Hello, World!"

    # FixedString(N) 0x16<var_uint_size>
    assert select.("'Hello'::FixedString(5)") == ["Hello", "FixedString(5)"]
    assert select.("'Hell'::FixedString(5)") == ["Hell\0", "FixedString(5)"]

    # TODO
    # Enum8	0x17<var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int8_value_1>...<var_uint_name_size_N><name_data_N><int8_value_N>
    assert_raise ArgumentError, "unsupported dynamic type Enum8", fn ->
      select.("'a'::Enum8('a' = 1, 'b' = 2, 'c' = 3)")
    end

    # TODO
    # Enum16 0x18<var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int16_little_endian_value_1>...><var_uint_name_size_N><name_data_N><int16_little_endian_value_N>
    assert_raise ArgumentError, "unsupported dynamic type Enum16", fn ->
      select.("'a'::Enum16('a' = 1, 'b' = 2, 'c' = 3)")
    end

    # Decimal32(P, S) 0x19<uint8_precision><uint8_scale>
    assert select.("42.42::Decimal32(2)") == [Decimal.new("42.42"), "Decimal(9, 2)"]

    # Decimal64(P, S) 0x1A<uint8_precision><uint8_scale>
    assert select.("-42.42::Decimal64(2)") == [Decimal.new("-42.42"), "Decimal(18, 2)"]

    # Decimal128(P, S) 0x1B<uint8_precision><uint8_scale>
    assert select.("1234567890.123456789::Decimal128(9)") ==
             [Decimal.new("1234567890.123456789"), "Decimal(38, 9)"]

    # Decimal256(P, S) 0x1C<uint8_precision><uint8_scale>
    assert select.("-1234567890.123456789::Decimal256(9)") ==
             [Decimal.new("-1234567890.123456789"), "Decimal(76, 9)"]

    # UUID 0x1D
    assert select.("'550e8400-e29b-41d4-a716-446655440000'::UUID") ==
             [Ecto.UUID.dump!("550e8400-e29b-41d4-a716-446655440000"), "UUID"]

    # Array(T) 0x1E<nested_type_encoding>
    assert select.("[1, 2, 3]::Array(UInt8)") == [[1, 2, 3], "Array(UInt8)"]
    assert select.("[1, 2, 3]::Array(Int64)") == [[1, 2, 3], "Array(Int64)"]

    assert select.("['hello', 'world', '!']::Array(String)") == [
             ["hello", "world", "!"],
             "Array(String)"
           ]

    assert select.("['hello', 'world', '!']::Array(LowCardinality(String))") == [
             ["hello", "world", "!"],
             "Array(LowCardinality(String))"
           ]

    assert select.("['hello', 'world', null, '!']::Array(Nullable(String))") == [
             ["hello", "world", nil, "!"],
             "Array(Nullable(String))"
           ]

    assert select.("[]::Array(Nothing)") == [[], "Array(Nothing)"]

    assert select.("[[1,2,3], [1,2], [3]]::Array(Array(UInt8))") == [
             [[1, 2, 3], [1, 2], [3]],
             "Array(Array(UInt8))"
           ]

    assert select.("[[[1],[],[2],[3,4,5]], [[1,2],[]], [[3]]]::Array(Array(Array(UInt8)))") == [
             [[[1], [], [2], [3, 4, 5]], [[1, 2], []], [[3]]],
             "Array(Array(Array(UInt8)))"
           ]

    assert select.("['2020-01-01', '2023-01-01']::Array(Date)") == [
             [~D[2020-01-01], ~D[2023-01-01]],
             "Array(Date)"
           ]

    # TODO
    # Tuple(T1, ..., TN) 0x1F<var_uint_number_of_elements><nested_type_encoding_1>...<nested_type_encoding_N>
    assert_raise ArgumentError, "unsupported dynamic type Tuple", fn ->
      select.("('a', 'b', 'c')::Tuple(String, String, String)")
    end

    # TODO
    # Tuple(name1 T1, ..., nameN TN) 0x20<var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
    assert_raise ArgumentError, "unsupported dynamic type TupleWithNames", fn ->
      select.("('a' = 'b', 'c' = 'd')::Tuple(a String, c String)")
    end

    # TODO
    # Set 0x21

    # TODO
    # Interval 0x22<interval_kind> (see interval kind binary encoding)

    # Nullable(T) 0x23<nested_type_encoding>
    assert select.("'Hello, World!'::Nullable(String)") == ["Hello, World!", "String"]
    assert select.("null::Nullable(String)") == [nil, "None"]

    # TODO
    # Function 0x24<var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N><return_type_encoding>

    # TODO
    # AggregateFunction(function_name(param_1, ..., param_N), arg_T1, ..., arg_TN) 0x25<var_uint_version><var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)

    # LowCardinality(T)	0x26<nested_type_encoding>
    assert select.("'Hello, World!'::LowCardinality(String)") == [
             "Hello, World!",
             "LowCardinality(String)"
           ]

    # TODO
    # Map(K, V) 0x27<key_type_encoding><value_type_encoding>
    assert_raise ArgumentError, "unsupported dynamic type Map", fn ->
      select.("map('key1', 'value1', 'key2', 'value2')::Map(String, String)")
    end

    # IPv4 0x28
    assert select.("'1.1.1.1'::IPv4") == [{1, 1, 1, 1}, "IPv4"]

    # IPv6 0x29
    assert select.("'::1'::IPv6") == [{0, 0, 0, 0, 0, 0, 0, 1}, "IPv6"]

    # TODO
    # Variant(T1, ..., TN) 0x2A<var_uint_number_of_variants><variant_type_encoding_1>...<variant_type_encoding_N>
    assert_raise ArgumentError, "unsupported dynamic type Variant", fn ->
      select.("['a', 1]::Array(Variant(String, UInt8))")
    end

    # TODO
    # Dynamic(max_types=N) 0x2B<uint8_max_types>

    # TODO
    # Custom type (Ring, Polygon, etc) 0x2C<var_uint_type_name_size><type_name_data>
    assert_raise ArgumentError, "unsupported dynamic type CustomType", fn ->
      select.("(0, 1)::Point")
    end

    # Bool 0x2D
    assert select.("true") == [true, "Bool"]
    assert select.("false") == [false, "Bool"]

    # TODO
    # SimpleAggregateFunction(function_name(param_1, ..., param_N), arg_T1, ..., arg_TN)	0x2E<var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
    # Nested(name1 T1, ..., nameN TN)	0x2F<var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
    # JSON(max_dynamic_paths=N, max_dynamic_types=M, path Type, SKIP skip_path, SKIP REGEXP skip_path_regexp)	0x30<uint8_serialization_version><var_int_max_dynamic_paths><uint8_max_dynamic_types><var_uint_number_of_typed_paths><var_uint_path_name_size_1><path_name_data_1><encoded_type_1>...<var_uint_number_of_skip_paths><var_uint_skip_path_size_1><skip_path_data_1>...<var_uint_number_of_skip_path_regexps><var_uint_skip_path_regexp_size_1><skip_path_data_regexp_1>...
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#creating-dynamic
  test "creating dynamic", %{conn: conn} do
    # Using Dynamic type in table column definition:
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);")

    assert Ch.query!(conn, "SELECT d, dynamicType(d) FROM test;").rows == [
             [nil, "None"],
             [42, "Int64"],
             ["Hello, World!", "String"],
             [[1, 2, 3], "Array(Int64)"]
           ]

    # Using CAST from ordinary column:
    assert Ch.query!(conn, "SELECT 'Hello, World!'::Dynamic AS d, dynamicType(d);").rows == [
             ["Hello, World!", "String"]
           ]

    # Using CAST from Variant column:
    assert Ch.query!(
             conn,
             "SELECT multiIf((number % 3) = 0, number, (number % 3) = 1, range(number + 1), NULL)::Dynamic AS d, dynamicType(d) FROM numbers(3)",
             [],
             settings: [
               enable_variant_type: 1,
               use_variant_as_common_type: 1
             ]
           ).rows == [
             [0, "UInt64"],
             [[0, 1], "Array(UInt64)"],
             [nil, "None"]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#reading-dynamic-nested-types-as-subcolumns
  test "reading dynamic nested types as subcolumns", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);")

    assert Ch.query!(
             conn,
             "SELECT d, dynamicType(d), d.String, d.Int64, d.`Array(Int64)`, d.Date, d.`Array(String)` FROM test;"
           ).rows == [
             [nil, "None", nil, nil, [], nil, []],
             [42, "Int64", nil, 42, [], nil, []],
             ["Hello, World!", "String", "Hello, World!", nil, [], nil, []],
             [[1, 2, 3], "Array(Int64)", nil, nil, [1, 2, 3], nil, []]
           ]

    assert Ch.query!(
             conn,
             "SELECT toTypeName(d.String), toTypeName(d.Int64), toTypeName(d.`Array(Int64)`), toTypeName(d.Date), toTypeName(d.`Array(String)`)  FROM test LIMIT 1;"
           ).rows == [
             [
               "Nullable(String)",
               "Nullable(Int64)",
               "Array(Int64)",
               "Nullable(Date)",
               "Array(String)"
             ]
           ]

    assert Ch.query!(
             conn,
             "SELECT d, dynamicType(d), dynamicElement(d, 'String'), dynamicElement(d, 'Int64'), dynamicElement(d, 'Array(Int64)'), dynamicElement(d, 'Date'), dynamicElement(d, 'Array(String)') FROM test;"
           ).rows == [
             [nil, "None", nil, nil, [], nil, []],
             [42, "Int64", nil, 42, [], nil, []],
             ["Hello, World!", "String", "Hello, World!", nil, [], nil, []],
             [[1, 2, 3], "Array(Int64)", nil, nil, [1, 2, 3], nil, []]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-string-column-to-a-dynamic-column-through-parsing
  test "converting a string column to a dynamic column through parsing", %{conn: conn} do
    assert Ch.query!(
             conn,
             "SELECT CAST(materialize(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01')), 'Map(String, Dynamic)') as map_of_dynamic, mapApply((k, v) -> (k, dynamicType(v)), map_of_dynamic) as map_of_dynamic_types;",
             [],
             settings: [cast_string_to_dynamic_use_inference: 1]
           ).rows == [
             [
               %{"key1" => 42, "key2" => true, "key3" => ~D[2020-01-01]},
               %{"key1" => "Int64", "key2" => "Bool", "key3" => "Date"}
             ]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-dynamic-column-to-an-ordinary-column
  test "converting a dynamic column to an ordinary column", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (d Dynamic) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('42.42'), (true), ('e10');")

    assert Ch.query!(conn, "SELECT d::Nullable(Float64) FROM test;").rows == [
             [nil],
             [42.0],
             [42.42],
             [1.0],
             [0.0]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-variant-column-to-dynamic-column
  test "converting a variant column to dynamic column", %{conn: conn} do
    Ch.query!(
      conn,
      "CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;"
    )

    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), ('String'), ([1, 2, 3]);")

    assert Ch.query!(conn, "SELECT v::Dynamic AS d, dynamicType(d) FROM test;").rows == [
             [nil, "None"],
             [42, "UInt64"],
             ["String", "String"],
             [[1, 2, 3], "Array(UInt64)"]
           ]
  end

  # https://clickhouse.com/docs/sql-reference/data-types/dynamic#converting-a-dynamicmax_typesn-column-to-another-dynamicmax_typesk
  test "converting a Dynamic(max_types=N) column to another Dynamic(max_types=K)", %{conn: conn} do
    Ch.query!(conn, "CREATE TABLE test (d Dynamic(max_types=4)) ENGINE = Memory;")
    on_exit(fn -> Ch.Test.query("DROP TABLE test", [], database: Ch.Test.database()) end)
    Ch.query!(conn, "INSERT INTO test VALUES (NULL), (42), (43), ('42.42'), (true), ([1, 2, 3]);")

    assert Ch.query!(conn, "SELECT d::Dynamic(max_types=5) as d2, dynamicType(d2) FROM test;").rows ==
             [
               [nil, "None"],
               [42, "Int64"],
               [43, "Int64"],
               ["42.42", "String"],
               [true, "Bool"],
               [[1, 2, 3], "Array(Int64)"]
             ]

    assert Ch.query!(
             conn,
             "SELECT d, dynamicType(d), d::Dynamic(max_types=2) as d2, dynamicType(d2), isDynamicElementInSharedData(d2) FROM test;"
           ).rows == [
             [nil, "None", nil, "None", false],
             [42, "Int64", 42, "Int64", false],
             [43, "Int64", 43, "Int64", false],
             ["42.42", "String", "42.42", "String", false],
             [true, "Bool", true, "Bool", true],
             [[1, 2, 3], "Array(Int64)", [1, 2, 3], "Array(Int64)", true]
           ]
  end
end
