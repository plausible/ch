defmodule Ch.RowBinaryTest do
  use ExUnit.Case, async: true
  doctest Ch.RowBinary, import: true
  import Ch.RowBinary
  import Bitwise

  test "encode -> decode" do
    spec = [
      {:string, ""},
      {:string, "a"},
      {:string, String.duplicate("a", 500)},
      {:string, String.duplicate("a", 15000)},
      {{:fixed_string, 2}, <<0, 0>>},
      {{:fixed_string, 2}, "a" <> <<0>>},
      {{:fixed_string, 2}, "aa"},
      {:u8, 0},
      {:u8, 0xFF},
      {:u16, 0},
      {:u16, 0xFFFF},
      {:u32, 0},
      {:u32, 0xFFFFFFFF},
      {:u64, 0},
      {:u64, 0xFFFFFFFFFFFFFFFF},
      {:u128, 0},
      {:u128, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF},
      {:u256, 0},
      {:u256, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF},
      {:i8, -0x80},
      {:i8, 0},
      {:i8, 0x7F},
      {:i16, -0x8000},
      {:i16, 0},
      {:i16, 0x7FFF},
      {:i32, -0x80000000},
      {:i32, 0},
      {:i32, 0x7FFFFFFF},
      {:i64, -0x800000000000000},
      {:i64, 0},
      {:i64, 0x7FFFFFFFFFFFFFFF},
      {:i128, -0x800000000000000000000000000000},
      {:i128, 0},
      {:i128, 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF},
      {:i256, -0x800000000000000000000000000000000000000000000000000000000000},
      {:i256, 0},
      {:i256, 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF},
      {:f32, 1.2345678806304932},
      {:f64, 1.234567898762738492830000503040030202020433},
      {:date, ~D[2022-01-01]},
      {:date, ~D[2042-01-01]},
      {:date, ~D[1970-01-01]},
      {:date32, ~D[1960-01-01]},
      {:date32, ~D[2100-01-01]},
      {:datetime, ~N[1970-01-01 00:00:00]},
      {:datetime, ~N[2022-01-01 00:00:00]},
      {:datetime, ~N[2042-01-01 00:00:00]},
      {{:array, :string}, []},
      {{:array, :string},
       [
         "",
         "a",
         String.duplicate("a", 500),
         String.duplicate("a", 15000)
       ]},
      {{:array, :u8}, [0, 0xFF]},
      {{:array, :u16}, [0, 0xFFFF]},
      {{:array, :u32}, [0, 0xFFFFFFFF]},
      {{:array, :u64}, [0, 0xFFFFFFFFFFFFFFFF]},
      {{:array, :i8}, [-0x80, 0, 0x7F]},
      {{:array, :i16}, [-0x8000, 0, 0x7FFF]},
      {{:array, :i32}, [-0x80000000, 0, 0x7FFFFFFF]},
      {{:array, :i64}, [-0x800000000000000, 0, 0x7FFFFFFFFFFFFFFF]},
      {{:array, :f32}, [-1.2345678806304932, 0, 1.2345678806304932]},
      {{:array, :f64},
       [
         -1.234567898762738492830000503040030202020433,
         0,
         1.234567898762738492830000503040030202020433
       ]},
      {{:array, :date}, [~D[2022-01-01], ~D[2042-01-01], ~D[1970-01-01]]},
      {{:array, :datetime},
       [~N[1970-01-01 12:23:34], ~N[2022-01-01 22:12:59], ~N[2042-01-01 04:23:01]]},
      {{:array, {:array, :string}}, [["a"], [], ["a", "b"]]},
      {{:nullable, :string}, nil},
      {{:nullable, :string}, "string"},
      {:point, {10, 10}},
      {:point, {10.5, 11}},
      {{:map, :string, :string}, %{"a" => "b", "c" => "d"}}
    ]

    num_cols = length(spec)
    {types, row} = Enum.unzip(spec)

    encoded = [
      num_cols,
      Enum.map(1..num_cols, fn col -> encode(:string, "col#{col}") end),
      Enum.map(types, fn type -> encode(:string, Ch.Types.encode(type)) end),
      encode_row(row, types)
    ]

    [decoded_row] = decode_rows(IO.iodata_to_binary(encoded))

    for {original, decoded} <- Enum.zip(row, decoded_row) do
      assert original == decoded
    end
  end

  describe "encode/2" do
    test "decimal" do
      type = {:decimal32, _scale = 4}
      assert encode(type, Decimal.new("2")) == <<20000::32-little>>
      assert encode(type, Decimal.new("2.66")) == <<26600::32-little>>
      assert encode(type, Decimal.new("2.6666")) == <<26666::32-little>>
      assert encode(type, Decimal.new("2.66666")) == <<26667::32-little>>
    end

    test "uuid" do
      uuid = <<210, 189, 94, 201, 253, 197, 165, 63, 50, 181, 232, 82, 246, 58, 95, 9>>

      assert encode(:uuid, uuid) ==
               <<63, 165, 197, 253, 201, 94, 189, 210, 9, 95, 58, 246, 82, 232, 181, 50>>

      hex = "d2bd5ec9-fdc5-a53f-32b5-e852f63a5f09"
      assert encode(:uuid, hex) == encode(:uuid, uuid)
    end

    test "map" do
      assert encode({:map, :string, :string}, []) == 0
      assert encode({:map, :string, :string}, %{}) == 0

      assert encode({:map, :string, :string}, %{"hello" => "world"}) ==
               encode({:map, :string, :string}, [{"hello", "world"}])
    end

    test "nil" do
      assert encode({:nullable, :string}, nil) == 1
      assert encode(:string, nil) == 0
      assert encode({:fixed_string, _size = 2}, nil) == <<0, 0>>
      assert encode(:u8, nil) == 0
      assert encode(:u16, nil) == <<0, 0>>
      assert encode(:u32, nil) == <<0, 0, 0, 0>>
      assert encode(:u64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:i8, nil) == 0
      assert encode(:i16, nil) == <<0, 0>>
      assert encode(:i32, nil) == <<0, 0, 0, 0>>
      assert encode(:i64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:f32, nil) == <<0, 0, 0, 0>>
      assert encode(:f64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:boolean, nil) == 0
      assert encode({:array, :string}, nil) == 0
      assert encode(:date, nil) == <<0, 0>>
      assert encode(:date32, nil) == <<0, 0, 0, 0>>
      assert encode(:datetime, nil) == <<0, 0, 0, 0>>
      assert encode({:datetime64, :microsecond}, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:uuid, nil) == <<0::128>>
      assert encode({:decimal32, _scale = 4}, nil) == <<0::32>>
      assert encode({:decimal64, _scale = 4}, nil) == <<0::64>>
      assert encode({:decimal128, _scale = 4}, nil) == <<0::128>>
      assert encode({:decimal256, _scale = 4}, nil) == <<0::256>>
      assert encode(:point, nil) == <<0::128>>
      assert encode(:ring, nil) == 0
      assert encode(:polygon, nil) == 0
      assert encode(:multipolygon, nil) == 0
      assert encode({:map, :string, :string}, nil) == 0
    end
  end

  test "utf8" do
    # example from https://clickhouse.com/docs/en/sql-reference/functions/string-functions/#tovalidutf8
    value = "\x61\xF0\x80\x80\x80b"
    bin = IO.iodata_to_binary(encode(:binary, value))
    str = IO.iodata_to_binary(encode(:string, value))

    # encoding is the same since we don't want to modify the values implicitly
    assert bin == str

    # but decoding is different based on what type is provided
    assert decode_rows(str, [:string]) == [["a�b"]]
    assert decode_rows(bin, [:string]) == [["a�b"]]
    assert decode_rows(str, [:binary]) == [["\x61\xF0\x80\x80\x80b"]]
    assert decode_rows(bin, [:binary]) == [["\x61\xF0\x80\x80\x80b"]]

    path = "/some/url" <> <<0xAE>> <> "-/"
    assert decode_rows(<<byte_size(path), path::bytes>>, [:string]) == [["/some/url�-/"]]

    path = <<0xAF>> <> "/some/url" <> <<0xAE, 0xFE>> <> "-/" <> <<0xFA>>
    assert decode_rows(<<byte_size(path), path::bytes>>, [:string]) == [["�/some/url�-/�"]]

    path = "/opportunity/category/جوائز-ومسابقات"
    assert decode_rows(<<byte_size(path), path::bytes>>, [:string]) == [[path]]

    path = "/ﺝﻭﺎﺋﺯ-ﻮﻤﺳﺎﺒﻗﺎﺗ"
    assert decode_rows(<<byte_size(path), path::bytes>>, [:string]) == [[path]]
  end

  describe "decoding_types/1" do
    test "decodes supported types" do
      spec = [
        {"UInt8", :u8},
        {"UInt16", :u16},
        {"UInt32", :u32},
        {"UInt64", :u64},
        {"UInt128", :u128},
        {"UInt256", :u256},
        {"Int8", :i8},
        {"Int16", :i16},
        {"Int32", :i32},
        {"Int64", :i64},
        {"Int128", :i128},
        {"Int256", :i256},
        {"Float32", :f32},
        {"Float64", :f64},
        {"Decimal(9, 4)", {:decimal, _size = 32, _scale = 4}},
        {"Decimal(23, 11)", {:decimal, _size = 128, _scale = 11}},
        {"Bool", :boolean},
        {"String", :string},
        {"FixedString(2)", {:fixed_string, _size = 2}},
        {"FixedString(22)", {:fixed_string, _size = 22}},
        {"FixedString(222)", {:fixed_string, _size = 222}},
        {"UUID", :uuid},
        {"Date", :date},
        {"Date32", :date32},
        {"DateTime", {:datetime, nil}},
        {"DateTime('UTC')", {:datetime, "UTC"}},
        {"DateTime('Asia/Tokyo')", {:datetime, "Asia/Tokyo"}},
        {"DateTime64(6)", {:datetime64, 1_000_000, nil}},
        {"DateTime64(3, 'UTC')", {:datetime64, 1000, "UTC"}},
        {"DateTime64(9, 'Asia/Tokyo')", {:datetime64, 1_000_000_000, "Asia/Tokyo"}},
        {"Enum8('a' = 1, 'b' = 2)", {:enum8, %{1 => "a", 2 => "b"}}},
        {"Enum16('hello' = 2, 'world' = 3)", {:enum16, %{2 => "hello", 3 => "world"}}},
        {"LowCardinality(String)", :string},
        {"LowCardinality(FixedString(2))", {:fixed_string, _size = 2}},
        {"LowCardinality(Date)", :date},
        {"LowCardinality(DateTime)", {:datetime, nil}},
        {"LowCardinality(UInt64)", :u64},
        {"Array(String)", {:array, :string}},
        {"Array(Array(String))", {:array, {:array, :string}}},
        {"Array(FixedString(2))", {:array, {:fixed_string, _size = 2}}},
        {"Array(LowCardinality(String))", {:array, :string}},
        {"Array(Enum8('hello' = 2, 'world' = 3))",
         {:array, {:enum8, %{2 => "hello", 3 => "world"}}}},
        {"Array(Nothing)", {:array, :nothing}},
        {"Nullable(String)", {:nullable, :string}},
        {"Nullable(Float64)", {:nullable, :f64}},
        {"Nothing", :nothing}
      ]

      Enum.each(spec, fn {encoded, decoded} ->
        assert decoding_types([encoded]) == [decoded]
      end)
    end

    test "preserves order" do
      assert decoding_types(["UInt8", "UInt16"]) == [:u8, :u16]
    end
  end

  describe "decode_rows/1" do
    test "empty" do
      assert decode_rows(<<>>) == []
    end

    test "empty rows" do
      types = [:u8, :string]
      num_cols = length(types)

      encoded = [
        num_cols,
        Enum.map(1..num_cols, fn col -> encode(:string, "col#{col}") end),
        Enum.map(types, fn type -> encode(:string, Ch.Types.encode(type)) end)
      ]

      assert decode_rows(IO.iodata_to_binary(encoded)) == []
    end

    test "incomplete" do
      expected_message = """
      incomplete RowBinary data: ran out of bytes while decoding

      Expected to decode: [:u8]
      Remaining bytes: 0 bytes
      Partial row: [1]
      Completed rows: 0
      """

      assert_raise ArgumentError, expected_message, fn ->
        decode_rows(<<2, 1, "a", 1, "b", 5, "UInt8", 5, "UInt8", 1>>)
      end
    end

    test "nan floats" do
      payload =
        <<3, 164, 1, 114, 111, 117, 110, 100, 40, 100, 105, 118, 105, 100, 101, 40, 109, 117, 108,
          116, 105, 112, 108, 121, 40, 49, 48, 48, 44, 32, 112, 108, 117, 115, 40, 99, 111, 97,
          108, 101, 115, 99, 101, 40, 98, 111, 117, 110, 99, 101, 115, 44, 32, 48, 41, 44, 32, 99,
          111, 97, 108, 101, 115, 99, 101, 40, 100, 105, 118, 105, 100, 101, 40, 109, 117, 108,
          116, 105, 112, 108, 121, 40, 98, 111, 117, 110, 99, 101, 95, 114, 97, 116, 101, 44, 32,
          118, 105, 115, 105, 116, 115, 41, 44, 32, 49, 48, 48, 41, 44, 32, 48, 41, 41, 41, 44,
          32, 112, 108, 117, 115, 40, 99, 111, 97, 108, 101, 115, 99, 101, 40, 115, 49, 46, 118,
          105, 115, 105, 116, 115, 44, 32, 48, 41, 44, 32, 99, 111, 97, 108, 101, 115, 99, 101,
          40, 118, 105, 115, 105, 116, 115, 44, 32, 48, 41, 41, 41, 41, 100, 114, 111, 117, 110,
          100, 40, 100, 105, 118, 105, 100, 101, 40, 112, 108, 117, 115, 40, 115, 49, 46, 118,
          105, 115, 105, 116, 95, 100, 117, 114, 97, 116, 105, 111, 110, 44, 32, 109, 117, 108,
          116, 105, 112, 108, 121, 40, 118, 105, 115, 105, 116, 95, 100, 117, 114, 97, 116, 105,
          111, 110, 44, 32, 118, 105, 115, 105, 116, 115, 41, 41, 44, 32, 112, 108, 117, 115, 40,
          118, 105, 115, 105, 116, 115, 44, 32, 115, 49, 46, 118, 105, 115, 105, 116, 115, 41, 41,
          44, 32, 49, 41, 14, 115, 97, 109, 112, 108, 101, 95, 112, 101, 114, 99, 101, 110, 116,
          7, 70, 108, 111, 97, 116, 54, 52, 7, 70, 108, 111, 97, 116, 54, 52, 7, 70, 108, 111, 97,
          116, 54, 52, 0, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0, 0, 0, 0,
          89, 64>>

      assert decode_rows(payload) == [[nil, nil, 100.0]]
    end
  end

  describe "decode_rows/2" do
    test "empty" do
      assert decode_rows(<<>>, [:u8, :string]) == []
    end

    test "non-empty" do
      assert decode_rows(<<1, 2>>, [:u8, :u8]) == [[1, 2]]
    end

    test "incomplete" do
      expected_message = """
      incomplete RowBinary data: ran out of bytes while decoding

      Expected to decode: [:u8]
      Remaining bytes: 0 bytes
      Partial row: [1]
      Completed rows: 0
      """

      assert_raise ArgumentError, expected_message, fn ->
        decode_rows(<<1>>, [:u8, :u8])
      end
    end
  end

  # TODO maybe use stream_data?
  describe "invalid arguments" do
    # https://github.com/plausible/ch/issues/166
    test "for UInt8" do
      assert_raise ArgumentError, "invalid UInt8: 256", fn -> encode(:u8, 256) end
      assert_raise ArgumentError, "invalid UInt8: -1", fn -> encode(:u8, -1) end
      assert_raise ArgumentError, "invalid UInt8: \"a\"", fn -> encode(:u8, "a") end
    end

    test "for Int8" do
      assert_raise ArgumentError, "invalid Int8: 128", fn -> encode(:i8, 128) end
      assert_raise ArgumentError, "invalid Int8: -129", fn -> encode(:i8, -129) end
      assert_raise ArgumentError, "invalid Int8: \"a\"", fn -> encode(:i8, "a") end
    end
  end

  describe "decode_header/1" do
    test "byte-by-byte" do
      header =
        IO.iodata_to_binary([
          encode(:varint, 3),
          [encode(:string, "col1"), encode(:string, "col2"), encode(:string, "col3")],
          [encode(:string, "UInt8"), encode(:string, "String"), encode(:string, "UInt64")]
        ])

      rows =
        IO.iodata_to_binary([
          [encode(:u8, 1), encode(:string, "a"), encode(:u64, 100)],
          [encode(:u8, 2), encode(:string, "b"), encode(:u64, 101)]
        ])

      for take <- 0..(byte_size(header) - 1) do
        chunk = String.slice(header, 0, take)
        assert decode_header(chunk) == :more
      end

      assert {:ok, ["col1", "col2", "col3"], types = [:u8, :string, :u64], rest} =
               decode_header(header <> rows)

      assert {rows, "", nil} = decode_rows_continue(rest, types, nil)
      assert rows == [[1, "a", 100], [2, "b", 101]]
    end
  end

  describe "decode_rows_continue/3" do
    defp byte_by_byte(binary, types) do
      byte_by_byte(binary, decoding_types(types), _rows = [], _buffer = "", _state = nil)
    end

    defp byte_by_byte(<<byte, rest::bytes>>, types, rows, buffer, state) do
      {new_rows, buffer, state} = decode_rows_continue(<<buffer::bytes, byte>>, types, state)
      byte_by_byte(rest, types, rows ++ new_rows, buffer, state)
    end

    defp byte_by_byte(<<>>, _types, rows, buffer, state) do
      assert buffer == ""
      assert state == nil
      rows
    end

    test "byte-by-byte with simple types" do
      binary =
        IO.iodata_to_binary([
          [encode(:u8, 1), encode(:string, "a"), encode(:u64, 100)],
          [encode(:u8, 2), encode(:string, "b"), encode(:u64, 200)]
        ])

      assert byte_by_byte(binary, [:u8, :string, :u64]) == [
               [1, "a", 100],
               [2, "b", 200]
             ]
    end

    test "byte-by-byte decode with nested arrays" do
      binary =
        IO.iodata_to_binary([
          [
            encode(:u8, 1),
            encode({:array, {:array, :string}}, [["abc", "def"], ["xyz"]])
          ],
          [
            encode(:u8, 2),
            encode({:array, {:array, :string}}, [["abc", "def", "xyz"]])
          ]
        ])

      assert byte_by_byte(binary, [:u8, {:array, {:array, :string}}]) == [
               [1, [["abc", "def"], ["xyz"]]],
               [2, [["abc", "def", "xyz"]]]
             ]
    end

    test "byte-by-byte with decimals" do
      binary =
        IO.iodata_to_binary([
          [
            encode({:decimal32, 4}, Decimal.new("12.3456")),
            encode({:decimal64, 4}, Decimal.new("78.9012"))
          ],
          [
            encode({:decimal32, 4}, Decimal.new("0.0001")),
            encode({:decimal64, 4}, Decimal.new("0.0002"))
          ]
        ])

      assert byte_by_byte(binary, [{:decimal32, 4}, {:decimal64, 4}]) == [
               [Decimal.new("12.3456"), Decimal.new("78.9012")],
               [Decimal.new("0.0001"), Decimal.new("0.0002")]
             ]
    end

    test "byte-by-byte with maps" do
      binary =
        IO.iodata_to_binary([
          [
            encode({:map, :string, :u32}, %{"a" => 1, "b" => 2}),
            encode({:map, :string, :string}, %{"key" => "value"})
          ],
          [
            encode({:map, :string, :u32}, %{"x" => 10, "y" => 20}),
            encode({:map, :string, :string}, %{"foo" => "bar"})
          ]
        ])

      assert byte_by_byte(binary, [{:map, :string, :u32}, {:map, :string, :string}]) == [
               [%{"a" => 1, "b" => 2}, %{"key" => "value"}],
               [%{"x" => 10, "y" => 20}, %{"foo" => "bar"}]
             ]
    end

    test "byte-by-byte with enums" do
      mapping8 = %{"a" => 1, "b" => 2}
      mapping16 = %{"x" => 10, "y" => 20}

      binary =
        IO.iodata_to_binary([
          [encode({:enum8, mapping8}, "a"), encode({:enum16, mapping16}, "y")],
          [encode({:enum8, mapping8}, "b"), encode({:enum16, mapping16}, "x")]
        ])

      assert byte_by_byte(binary, [{:enum8, mapping8}, {:enum16, mapping16}]) == [
               ["a", "y"],
               ["b", "x"]
             ]
    end

    test "byte-by-byte decode with nullable types" do
      binary =
        IO.iodata_to_binary([
          [encode({:nullable, :u32}, 100), encode({:nullable, :string}, "hello"), encode(:u8, 1)],
          [encode({:nullable, :u32}, nil), encode({:nullable, :string}, "world"), encode(:u8, 2)],
          [encode({:nullable, :u32}, 200), encode({:nullable, :string}, nil), encode(:u8, 3)]
        ])

      assert byte_by_byte(binary, [{:nullable, :u32}, {:nullable, :string}, :u8]) == [
               [100, "hello", 1],
               [nil, "world", 2],
               [200, nil, 3]
             ]
    end

    test "byte-by-byte decode with fixed strings" do
      binary =
        IO.iodata_to_binary([
          [encode({:fixed_string, 5}, "hello"), encode({:fixed_string, 3}, "abc")],
          [encode({:fixed_string, 5}, "world"), encode({:fixed_string, 3}, "xyz")]
        ])

      assert byte_by_byte(binary, [{:fixed_string, 5}, {:fixed_string, 3}]) == [
               ["hello", "abc"],
               ["world", "xyz"]
             ]
    end

    test "byte-by-byte decode with json" do
      binary =
        IO.iodata_to_binary([
          [encode(:json, %{"key" => "value"}), encode(:json, [1, 2, 3])],
          [encode(:json, nil), encode(:json, %{"another_key" => 42})]
        ])

      assert byte_by_byte(binary, [:json, :json]) == [
               [%{"key" => "value"}, [1, 2, 3]],
               [nil, %{"another_key" => 42}]
             ]
    end

    test "byte-by-byte decode with boolean" do
      binary =
        IO.iodata_to_binary([
          [encode(:boolean, true), encode(:boolean, false), encode(:boolean, true)],
          [encode(:boolean, false), encode(:boolean, false), encode(:boolean, true)]
        ])

      assert byte_by_byte(binary, [:boolean, :boolean, :boolean]) == [
               [true, false, true],
               [false, false, true]
             ]
    end

    test "byte-by-byte decode with date and datetime" do
      binary =
        IO.iodata_to_binary([
          [
            encode(:date, ~D[2022-01-01]),
            encode(:datetime, ~N[2022-01-01 12:00:00]),
            encode(:datetime, ~U[2022-01-01 12:00:00Z])
          ],
          [
            encode(:date, ~D[2042-12-31]),
            encode(:datetime, ~N[2042-12-31 23:59:59]),
            encode(:datetime, ~U[2042-12-31 23:59:59Z])
          ]
        ])

      assert byte_by_byte(binary, [:date, :datetime, {:datetime, "UTC"}]) == [
               [~D[2022-01-01], ~N[2022-01-01 12:00:00], ~U[2022-01-01 12:00:00Z]],
               [~D[2042-12-31], ~N[2042-12-31 23:59:59], ~U[2042-12-31 23:59:59Z]]
             ]
    end

    test "byte-by-byte decode with all integer types" do
      binary =
        IO.iodata_to_binary([
          [
            encode(:u8, 0),
            encode(:u16, 0),
            encode(:u32, 0),
            encode(:u64, 0),
            encode(:u128, 0),
            encode(:u256, 0),
            encode(:i8, 0),
            encode(:i16, 0),
            encode(:i32, 0),
            encode(:i64, 0),
            encode(:i128, 0),
            encode(:i256, 0)
          ],
          [
            encode(:u8, (1 <<< 8) - 1),
            encode(:u16, (1 <<< 16) - 1),
            encode(:u32, (1 <<< 32) - 1),
            encode(:u64, (1 <<< 64) - 1),
            encode(:u128, (1 <<< 128) - 1),
            encode(:u256, (1 <<< 256) - 1),
            encode(:i8, -(1 <<< 7)),
            encode(:i16, -(1 <<< 15)),
            encode(:i32, -(1 <<< 31)),
            encode(:i64, -(1 <<< 63)),
            encode(:i128, -(1 <<< 127)),
            encode(:i256, -(1 <<< 255))
          ]
        ])

      assert byte_by_byte(binary, [
               :u8,
               :u16,
               :u32,
               :u64,
               :u128,
               :u256,
               :i8,
               :i16,
               :i32,
               :i64,
               :i128,
               :i256
             ]) == [
               [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
               [
                 255,
                 65535,
                 4_294_967_295,
                 18_446_744_073_709_551_615,
                 340_282_366_920_938_463_463_374_607_431_768_211_455,
                 115_792_089_237_316_195_423_570_985_008_687_907_853_269_984_665_640_564_039_457_584_007_913_129_639_935,
                 -128,
                 -32768,
                 -2_147_483_648,
                 -9_223_372_036_854_775_808,
                 -170_141_183_460_469_231_731_687_303_715_884_105_728,
                 -57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_968
               ]
             ]
    end

    test "byte-by-byte decode with floats" do
      binary =
        IO.iodata_to_binary([
          [encode(:f32, 3.14159), encode(:f64, 2.718281828459045)],
          [encode(:f32, -1.5), encode(:f64, 0.0)]
        ])

      assert byte_by_byte(binary, [:f32, :f64]) == [
               [3.141590118408203, 2.718281828459045],
               [-1.5, 0.0]
             ]
    end
  end
end
