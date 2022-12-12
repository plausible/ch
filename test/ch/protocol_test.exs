defmodule Ch.ProtocolTest do
  use ExUnit.Case, async: true
  import Ch.Protocol

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
       [~N[1970-01-01 12:23:34], ~N[2022-01-01 22:12:59], ~N[2042-01-01 04:23:01]]}
      # TODO
      # {{:array, {:array, :string}}, [["a"], [], ["a", "b"]]}
    ]

    cols = length(spec)
    {types, row} = Enum.unzip(spec)

    header = [
      Enum.map(1..cols, fn col -> "col#{col}" end),
      Enum.map(types, &dump_type/1)
    ]

    encoded = encode_row(row, types)

    bin =
      IO.iodata_to_binary([
        cols,
        encode_rows(header, List.duplicate(:string, cols)),
        encoded
      ])

    [decoded] = decode_rows(bin)

    for {original, decoded} <- Enum.zip(row, decoded) do
      assert original == decoded
    end
  end

  describe "decode_rows/1" do
    test "accepts empty bin" do
      assert decode_rows(<<>>) == []
    end
  end

  test "encode nil" do
    assert encode(:varint, nil) == 0
    assert encode(:string, nil) == 0
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
    assert encode(:boolean, nil) == 0
    assert encode({:array, :string}, nil) == 0
    assert encode(:date, nil) == <<0, 0>>
    assert encode(:datetime, nil) == <<0, 0, 0, 0>>
  end
end
