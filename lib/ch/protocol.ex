defmodule Ch.Protocol do
  @moduledoc false
  import Bitwise
  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  def encode_row([el | els], [type | types]), do: [encode(type, el) | encode_row(els, types)]
  def encode_row([] = done, []), do: done

  def encode_rows([row | rows], types), do: [encode_row(row, types) | encode_rows(rows, types)]
  def encode_rows([] = done, _types), do: done

  # TODO
  def encode(:varint, num) when num < 128, do: <<num>>
  def encode(:varint, num), do: <<1::1, num::7, encode(:varint, num >>> 7)::bytes>>

  def encode(:string, str) do
    [encode(:varint, byte_size(str)) | str]
  end

  def encode(:u8, i), do: <<i::little>>
  def encode(:u16, i), do: <<i::16-little>>
  def encode(:u32, i), do: <<i::32-little>>
  def encode(:u64, i), do: <<i::64-little>>

  def encode(:i8, i), do: <<i::little-signed>>
  def encode(:i16, i), do: <<i::16-little-signed>>
  def encode(:i32, i), do: <<i::32-little-signed>>
  def encode(:i64, i), do: <<i::64-little-signed>>

  def encode(:f64, f), do: <<f::64-little-signed-float>>
  def encode(:f32, f), do: <<f::32-little-signed-float>>

  def encode(:boolean, true), do: encode(:u8, 1)
  def encode(:boolean, false), do: encode(:u8, 0)

  def encode({:array, type}, l) do
    [encode(:varint, length(l)) | encode_many(l, type)]
  end

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end

  defp encode_many([el | rest], type), do: [encode(type, el) | encode_many(rest, type)]
  defp encode_many([] = done, _type), do: done

  def decode_rows(<<cols, rest::bytes>>), do: skip_names(rest, cols, cols)

  defp skip_names(rest, 0, count), do: decode_types(rest, count, _acc = [])

  # TODO proper varint
  defp skip_names(<<0::1, v::7, _::size(v)-bytes, rest::bytes>>, left, count) do
    skip_names(rest, left - 1, count)
  end

  defp decode_types(rows, 0, types) do
    types = :lists.reverse(types)
    _decode_rows(rows, types, [], [], types)
  end

  # TODO uf
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

  patterns = [
    # TODO proper varint
    {quote(do: <<0::1, v::7, s::size(v)-bytes>>), :string, quote(do: s)},
    {quote(do: <<1::1, v1::7, 0::1, v2::7, s::size((v2 <<< 7) + v1)-bytes>>), :string,
     quote(do: s)},
    {quote(do: <<u::little>>), :u8, quote(do: u)},
    {quote(do: <<u::16-little>>), :u16, quote(do: u)},
    {quote(do: <<u::32-little>>), :u32, quote(do: u)},
    {quote(do: <<u::64-little>>), :u64, quote(do: u)},
    {quote(do: <<i::little-signed>>), :i8, quote(do: i)},
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

  defp _decode_rows(rest, [], row, outer_acc, types) do
    _decode_rows(rest, types, [], [:lists.reverse(row) | outer_acc], types)
  end

  defp _decode_rows(<<>>, types, [], rows, types) do
    :lists.reverse(rows)
  end

  defp _decode_array(rest, _type, _count = 0, array, inner_types, inner_acc, outer_acc, types) do
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
