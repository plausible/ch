defmodule Ch.RowBinaryTest do
  alias Ch.RowBinary
  use ExUnit.Case, async: true
  import Ch.{RowBinary, Test}

  test "encode -> decode" do
    spec = [
      {:string, ""},
      {:string, "a"},
      {:string, String.duplicate("a", 500)},
      {:string, String.duplicate("a", 15000)},
      {:u8, 0},
      {:u8, 0xFF},
      {:u16, 0},
      {:u16, 0xFFFF},
      {:u32, 0},
      {:u32, 0xFFFFFFFF},
      {:u64, 0},
      {:u64, 0xFFFFFFFFFFFFFFFF},
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
      {:f32, 1.2345678806304932},
      {:f64, 1.234567898762738492830000503040030202020433},
      {:date, ~D[2022-01-01]},
      {:date, ~D[2042-01-01]},
      {:date, ~D[1970-01-01]},
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
      {{:nullable, :string}, "string"}
    ]

    num_cols = length(spec)
    {types, row} = Enum.unzip(spec)

    header = [
      Enum.map(1..num_cols, fn col -> "col#{col}" end),
      Enum.map(types, &dump_type/1)
    ]

    encoded =
      IO.iodata_to_binary([
        num_cols,
        encode_rows(header, List.duplicate(:string, num_cols)),
        encode_row(row, types)
      ])

    [decoded_row] = decode_rows(encoded)

    for {original, decoded} <- Enum.zip(row, decoded_row) do
      assert original == decoded
    end
  end

  describe "encode/2" do
    test "decimal" do
      assert encode({:decimal, 9, 4}, nil) == <<0::32>>
      assert encode({:decimal, 9, 4}, Decimal.new("2.66")) == <<26600::32-little>>
      assert encode({:decimal, 9, 4}, Decimal.new("2.6666")) == <<26666::32-little>>
      assert encode({:decimal, 9, 4}, Decimal.new("2.66666")) == <<26667::32-little>>
    end

    test "nil" do
      assert encode(:varint, nil) == <<0>>
      assert encode(:string, nil) == <<0>>
      assert encode({:string, 2}, nil) == <<0, 0>>
      assert encode(:u8, nil) == <<0>>
      assert encode(:u16, nil) == <<0, 0>>
      assert encode(:u32, nil) == <<0, 0, 0, 0>>
      assert encode(:u64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:i8, nil) == <<0>>
      assert encode(:i16, nil) == <<0, 0>>
      assert encode(:i32, nil) == <<0, 0, 0, 0>>
      assert encode(:i64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:f32, nil) == <<0, 0, 0, 0>>
      assert encode(:f64, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:boolean, nil) == <<0>>
      assert encode({:array, :string}, nil) == <<0>>
      assert encode(:date, nil) == <<0, 0>>
      assert encode(:date32, nil) == <<0, 0, 0, 0>>
      assert encode(:datetime, nil) == <<0, 0, 0, 0>>
      assert encode({:datetime64, :microsecond}, nil) == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert encode(:uuid, nil) == <<0::128>>
      assert encode({:nullable, :string}, nil) == <<1>>
    end
  end

  describe "decode_types/1" do
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
        {"Decimal(9, 4)", {:decimal, 9, 4}},
        {"Decimal(23, 11)", {:decimal, 23, 11}},
        {"Bool", :boolean},
        {"String", :string},
        {"FixedString(2)", {:string, 2}},
        {"FixedString(22)", {:string, 22}},
        {"FixedString(222)", {:string, 222}},
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
        {"LowCardinality(FixedString(2))", {:string, 2}},
        {"LowCardinality(Date)", :date},
        {"LowCardinality(DateTime)", {:datetime, nil}},
        {"LowCardinality(UInt64)", :u64},
        {"Array(String)", {:array, :string}},
        {"Array(Array(String))", {:array, {:array, :string}}},
        {"Array(FixedString(2))", {:array, {:string, 2}}},
        {"Array(LowCardinality(String))", {:array, :string}},
        {"Array(Enum8('hello' = 2, 'world' = 3))",
         {:array, {:enum8, %{2 => "hello", 3 => "world"}}}},
        {"Array(Nothing)", {:array, :nothing}},
        {"Nullable(String)", {:nullable, :string}},
        {"Nullable(Float64)", {:nullable, :f64}},
        {"Nothing", :nothing}
      ]

      Enum.each(spec, fn {encoded, decoded} ->
        assert decode_types([encoded]) == [decoded]
      end)
    end

    test "raises on unsupported types" do
      assert_raise ArgumentError, "Tuple(UInt8, String) type is not supported", fn ->
        decode_types(["Tuple(UInt8, String)"])
      end

      assert_raise ArgumentError, "Tuple(UInt8, Nullable(Nothing)) type is not supported", fn ->
        decode_types(["Tuple(UInt8, Nullable(Nothing))"])
      end

      assert_raise ArgumentError, "Map(String, UInt64) type is not supported", fn ->
        decode_types(["Map(String, UInt64)"])
      end
    end

    test "preserves order" do
      assert decode_types(["UInt8", "UInt16"]) == [:u8, :u16]
    end
  end

  describe "decode_rows/1" do
    test "empty" do
      assert decode_rows(<<>>) == []
    end

    test "empty rows" do
      types = [:u8, :string]
      num_cols = length(types)

      header = [
        Enum.map(1..num_cols, fn col -> "col#{col}" end),
        Enum.map(types, &dump_type/1)
      ]

      encoded =
        IO.iodata_to_binary([num_cols, encode_rows(header, List.duplicate(:string, num_cols))])

      assert decode_rows(encoded) == []
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
      assert RowBinary.decode_rows(<<>>, [:u8, :string]) == []
    end

    test "non-empty" do
      assert RowBinary.decode_rows(<<1, 2>>, [:u8, :u8]) == [[1, 2]]
    end
  end
end
