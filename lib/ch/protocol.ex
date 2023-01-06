defmodule Ch.Protocol do
  @moduledoc false
  # @compile {:bin_opt_info, true}
  import Bitwise
  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  def encode_row([el | els], [type | types]), do: [encode(type, el) | encode_row(els, types)]
  def encode_row([] = done, []), do: done

  def encode_rows([row | rows], types), do: encode_rows(row, types, rows, types)
  def encode_rows([] = done, _types), do: done

  defp encode_rows([el | els], [t | ts], rows, types) do
    [encode(t, el) | encode_rows(els, ts, rows, types)]
  end

  defp encode_rows([], [], rows, types), do: encode_rows(rows, types)

  # TODO
  def encode(:varint, num) when is_integer(num) and num < 128, do: <<num>>

  def encode(:varint, num) when is_integer(num) do
    [<<1::1, num::7>> | encode(:varint, num >>> 7)]
  end

  def encode(:varint, nil), do: 0

  def encode(:string, str) when is_binary(str) do
    [encode(:varint, byte_size(str)) | str]
  end

  def encode(:string, nil), do: 0

  def encode({:string, len}, str) when byte_size(str) == len do
    str
  end

  def encode({:string, len}, nil), do: <<0::size(len * 8)>>

  for size <- [8, 16, 32, 64] do
    def encode(unquote(:"u#{size}"), i) when is_integer(i) do
      <<i::unquote(size)-little>>
    end

    def encode(unquote(:"i#{size}"), i) when is_integer(i) do
      <<i::unquote(size)-little-signed>>
    end

    def encode(unquote(:"u#{size}"), nil), do: <<0::unquote(size)>>
    def encode(unquote(:"i#{size}"), nil), do: <<0::unquote(size)>>
  end

  for size <- [32, 64] do
    def encode(unquote(:"f#{size}"), f) when is_number(f) do
      <<f::unquote(size)-little-signed-float>>
    end

    def encode(unquote(:"f#{size}"), nil), do: <<0::unquote(size)>>
  end

  def encode(:boolean, true), do: 1
  def encode(:boolean, false), do: 0
  def encode(:boolean, nil), do: 0

  def encode({:array, type}, [_ | _] = l) do
    [encode(:varint, length(l)) | encode_many(l, type)]
  end

  def encode({:array, _type}, []), do: 0
  def encode({:array, _type}, nil), do: 0

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:datetime, nil), do: <<0::32>>

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end

  def encode(:date, nil), do: <<0::16>>

  defp encode_many([el | rest], type), do: [encode(type, el) | encode_many(rest, type)]
  defp encode_many([] = done, _type), do: done

  def decode_rows(<<cols, rest::bytes>>), do: skip_names(rest, cols, cols)
  def decode_rows(<<>>), do: []

  def decode_rows(<<data::bytes>>, types) do
    _decode_rows(data, types, [], [], types)
  end

  defp skip_names(<<rest::bytes>>, 0, count), do: decode_types(rest, count, _acc = [])

  # TODO proper varint
  defp skip_names(<<0::1, v::7, _::size(v)-bytes, rest::bytes>>, left, count) do
    skip_names(rest, left - 1, count)
  end

  defp decode_types(<<rest::bytes>>, 0, types) do
    types = :lists.reverse(types)
    _decode_rows(rest, types, [], [], types)
  end

  types = [
    {"String", :string},
    {"UInt8", :u8},
    {"UInt16", :u16},
    {"UInt32", :u32},
    {"UInt64", :u64},
    {"Int8", :i8},
    {"Int16", :i16},
    {"Int32", :i32},
    {"Int64", :i64},
    {"Float32", :f32},
    {"Float64", :f64},
    {"Date", :date},
    {"DateTime", :datetime},
    # TODO
    {"LowCardinality(String)", :string},
    {"LowCardinality(FixedString(2))", {:string, 2}},
    # TODO
    {"Array(String)", {:array, :string}},
    {"Array(UInt8)", {:array, :u8}},
    {"Array(UInt16)", {:array, :u16}},
    {"Array(UInt32)", {:array, :u32}},
    {"Array(UInt64)", {:array, :u64}},
    {"Array(Int8)", {:array, :i8}},
    {"Array(Int16)", {:array, :i16}},
    {"Array(Int32)", {:array, :i32}},
    {"Array(Int64)", {:array, :i64}},
    {"Array(Float32)", {:array, :f32}},
    {"Array(Float64)", {:array, :f64}},
    {"Array(Date)", {:array, :date}},
    {"Array(DateTime)", {:array, :datetime}}
  ]

  for {raw, type} <- types do
    defp decode_types(<<unquote(byte_size(raw)), unquote(raw)::bytes, rest::bytes>>, count, acc) do
      decode_types(rest, count - 1, [unquote(type) | acc])
    end
  end

  no_dump = ["LowCardinality(String)", "LowCardinality(FixedString(2))"]

  for {raw, type} <- Enum.reject(types, fn {raw, _} -> raw in no_dump end) do
    def dump_type(unquote(type)), do: unquote(raw)
  end

  patterns = [
    # TODO proper varint
    {quote(do: <<0::1, v::7, s::size(v)-bytes>>), :string, quote(do: s)},
    {quote(do: <<1::1, v1::7, 0::1, v2::7, s::size((v2 <<< 7) + v1)-bytes>>), :string,
     quote(do: s)},
    # TODO
    {quote(do: <<s::2-bytes>>), {:string, 2}, quote(do: s)},
    {quote(do: <<u>>), :u8, quote(do: u)},
    {quote(do: <<u::16-little>>), :u16, quote(do: u)},
    {quote(do: <<u::32-little>>), :u32, quote(do: u)},
    {quote(do: <<u::64-little>>), :u64, quote(do: u)},
    {quote(do: <<i::signed>>), :i8, quote(do: i)},
    {quote(do: <<i::16-little-signed>>), :i16, quote(do: i)},
    {quote(do: <<i::32-little-signed>>), :i32, quote(do: i)},
    {quote(do: <<i::64-little-signed>>), :i64, quote(do: i)},
    {quote(do: <<f::32-little-signed-float>>), :f32, quote(do: f)},
    {quote(do: <<f::64-little-signed-float>>), :f64, quote(do: f)},
    {quote(do: <<d::16-little>>), :date, quote(do: Date.add(@epoch_date, d))},
    {quote(do: <<s::32-little>>), :datetime,
     quote(do: NaiveDateTime.add(@epoch_naive_datetime, s))}
  ]

  for {pattern, type, value} <- patterns do
    defp _decode_rows(
           <<unquote(pattern), rest::bytes>>,
           [unquote(type) | inner_types],
           inner_acc,
           outer_acc,
           types
         ) do
      _decode_rows(rest, inner_types, [unquote(value) | inner_acc], outer_acc, types)
    end
  end

  # TODO proper varint
  defp _decode_rows(
         <<0::1, count::7, rest::bytes>>,
         [{:array, type} | inner_types],
         inner_acc,
         outer_acc,
         types
       ) do
    _decode_array(rest, type, count, [], inner_types, inner_acc, outer_acc, types)
  end

  defp _decode_rows(<<rest::bytes>>, [], row, outer_acc, types) do
    _decode_rows(rest, types, [], [:lists.reverse(row) | outer_acc], types)
  end

  defp _decode_rows(<<>>, types, [], rows, types) do
    :lists.reverse(rows)
  end

  defp _decode_array(
         <<rest::bytes>>,
         _type,
         _count = 0,
         array,
         inner_types,
         inner_acc,
         outer_acc,
         types
       ) do
    _decode_rows(rest, inner_types, [:lists.reverse(array) | inner_acc], outer_acc, types)
  end

  for {pattern, type, value} <- patterns do
    defp _decode_array(
           <<unquote(pattern), rest::bytes>>,
           unquote(type),
           count,
           array_acc,
           inner_types,
           inner_acc,
           outer_acc,
           types
         ) do
      _decode_array(
        rest,
        unquote(type),
        count - 1,
        [unquote(value) | array_acc],
        inner_types,
        inner_acc,
        outer_acc,
        types
      )
    end
  end
end
