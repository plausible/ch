defmodule Ch.RowBinary do
  @moduledoc false
  # @compile {:bin_opt_info, true}
  @dialyzer :no_improper_lists

  import Bitwise
  require Record

  Record.defrecord(:string, [:size])
  Record.defrecord(:decimal, [:size, :scale])
  Record.defrecord(:datetime64, [:unit])

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])
  @epoch_utc_datetime DateTime.new!(@epoch_date, ~T[00:00:00])

  def encode_row([el | els], [type | types]), do: [encode(type, el) | encode_row(els, types)]
  def encode_row([] = done, []), do: done

  def encode_rows([row | rows], types), do: encode_rows(row, types, rows, types)
  def encode_rows([] = done, _types), do: done

  defp encode_rows([el | els], [t | ts], rows, types) do
    [encode(t, el) | encode_rows(els, ts, rows, types)]
  end

  defp encode_rows([], [], rows, types), do: encode_rows(rows, types)

  def encode(:varint, num) when is_integer(num) and num < 128, do: <<num>>

  def encode(:varint, num) when is_integer(num) do
    [<<1::1, num::7>> | encode(:varint, num >>> 7)]
  end

  def encode(:varint, nil), do: <<0>>

  def encode(:string, str) when is_binary(str) do
    [encode(:varint, byte_size(str)) | str]
  end

  def encode(:string, nil), do: <<0>>

  def encode(string(size: size), nil), do: <<0::size(size * 8)>>

  def encode(string(size: size), str) when byte_size(str) == size do
    str
  end

  def encode(string(size: size), str) when byte_size(str) < size do
    to_pad = size - byte_size(str)
    [str | <<0::size(to_pad * 8)>>]
  end

  for size <- [8, 16, 32, 64, 128, 256] do
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

  def encode(decimal(size: size, scale: scale), %Decimal{sign: sign, coef: coef, exp: exp})
      when scale == abs(exp) do
    i = sign * coef
    <<i::size(size)-little>>
  end

  def encode(decimal(scale: scale) = t, %Decimal{} = d) do
    encode(t, Decimal.round(d, scale))
  end

  def encode(decimal(size: size), nil) do
    <<0::size(size)-little>>
  end

  def encode(:boolean, true), do: <<1>>
  def encode(:boolean, false), do: <<0>>
  def encode(:boolean, nil), do: <<0>>

  def encode({:array, type}, [_ | _] = l) do
    [encode(:varint, length(l)) | encode_many(l, type)]
  end

  def encode({:array, _type}, []), do: <<0>>
  def encode({:array, _type}, nil), do: <<0>>

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:datetime, %DateTime{} = datetime) do
    <<DateTime.diff(datetime, @epoch_utc_datetime)::32-little>>
  end

  def encode(:datetime, nil), do: <<0::32>>

  # TODO right now the timezones are ignored during encoding
  # assuming the user has provided the correct one
  def encode({:datetime = t, _timezone}, v) do
    encode(t, v)
  end

  def encode(datetime64(unit: unit), %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime, unit)::64-little-signed>>
  end

  def encode(datetime64(unit: unit), %DateTime{} = datetime) do
    <<DateTime.diff(datetime, @epoch_utc_datetime, unit)::64-little-signed>>
  end

  def encode(datetime64(), nil) do
    <<0::64>>
  end

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end

  def encode(:date, nil), do: <<0::16>>

  def encode(:date32, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::32-little-signed>>
  end

  def encode(:date32, nil), do: <<0::32>>

  def encode(:uuid, nil), do: <<0::128>>
  def encode(:uuid, <<u1::64, u2::64>>), do: <<u1::64-little, u2::64-little>>

  def encode(
        :uuid,
        <<a1, a2, a3, a4, a5, a6, a7, a8, ?-, b1, b2, b3, b4, ?-, c1, c2, c3, c4, ?-, d1, d2, d3,
          d4, ?-, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12>>
      ) do
    raw =
      <<d(a1)::4, d(a2)::4, d(a3)::4, d(a4)::4, d(a5)::4, d(a6)::4, d(a7)::4, d(a8)::4, d(b1)::4,
        d(b2)::4, d(b3)::4, d(b4)::4, d(c1)::4, d(c2)::4, d(c3)::4, d(c4)::4, d(d1)::4, d(d2)::4,
        d(d3)::4, d(d4)::4, d(e1)::4, d(e2)::4, d(e3)::4, d(e4)::4, d(e5)::4, d(e6)::4, d(e7)::4,
        d(e8)::4, d(e9)::4, d(e10)::4, d(e11)::4, d(e12)::4>>

    encode(:uuid, raw)
  end

  def encode({:nullable, _type}, nil), do: <<1>>
  def encode({:nullable, type}, value), do: [0 | encode(type, value)]

  defp encode_many([el | rest], type), do: [encode(type, el) | encode_many(rest, type)]
  defp encode_many([] = done, _type), do: done

  @compile {:inline, d: 1}

  defp d(?0), do: 0
  defp d(?1), do: 1
  defp d(?2), do: 2
  defp d(?3), do: 3
  defp d(?4), do: 4
  defp d(?5), do: 5
  defp d(?6), do: 6
  defp d(?7), do: 7
  defp d(?8), do: 8
  defp d(?9), do: 9
  defp d(?A), do: 10
  defp d(?B), do: 11
  defp d(?C), do: 12
  defp d(?D), do: 13
  defp d(?E), do: 14
  defp d(?F), do: 15
  defp d(?a), do: 10
  defp d(?b), do: 11
  defp d(?c), do: 12
  defp d(?d), do: 13
  defp d(?e), do: 14
  defp d(?f), do: 15

  def decode_rows(<<cols, rest::bytes>>), do: skip_names(rest, cols, cols)
  def decode_rows(<<>>), do: []

  def decode_rows(<<>>, _types), do: []

  def decode_rows(<<data::bytes>>, types) do
    decode_rows(types, data, [], [], types)
  end

  defp skip_names(<<rest::bytes>>, 0, count), do: decode_types(rest, count, _acc = [])

  varints = [
    {_pattern = quote(do: <<0::1, v1::7>>), _value = quote(do: v1)},
    {quote(do: <<1::1, v1::7, 0::1, v2::7>>), quote(do: (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 0::1, v3::7>>),
     quote(do: (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 0::1, v4::7>>),
     quote(do: (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 0::1, v5::7>>),
     quote(do: (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote(do: <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 0::1, v6::7>>),
     quote(do: (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1)},
    {quote do
       <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 0::1,
         v7::7>>
     end,
     quote do
       (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) + (v2 <<< 7) + v1
     end},
    {quote do
       <<1::1, v1::7, 1::1, v2::7, 1::1, v3::7, 1::1, v4::7, 1::1, v5::7, 1::1, v6::7, 1::1,
         v7::7, 0::1, v8::7>>
     end,
     quote do
       (v8 <<< 49) + (v7 <<< 42) + (v6 <<< 35) + (v5 <<< 28) + (v4 <<< 21) + (v3 <<< 14) +
         (v2 <<< 7) + v1
     end}
  ]

  for {pattern, value} <- varints do
    defp skip_names(<<unquote(pattern), _::size(unquote(value))-bytes, rest::bytes>>, left, count) do
      skip_names(rest, left - 1, count)
    end
  end

  defp decode_types(<<>>, 0, _types), do: []

  defp decode_types(<<rest::bytes>>, 0, types) do
    types = types |> decode_types() |> :lists.reverse()
    decode_rows(types, rest, _row = [], _rows = [], types)
  end

  defp decode_types(<<size, type::size(size)-bytes, rest::bytes>>, count, acc) do
    decode_types(rest, count - 1, [type | acc])
  end

  @doc false
  def decode_types([type | types]) do
    [decode_type(type) | decode_types(types)]
  end

  def decode_types([] = done), do: done

  scalar_types = [
    {"String", :string},
    {"UUID", :uuid},
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
    {"Date32", :date32},
    {"Bool", :boolean},
    {"Nothing", :nothing}
  ]

  for {encoded, decoded} <- scalar_types do
    defp decode_type(<<unquote(encoded)::bytes, _rest::bytes>>), do: unquote(decoded)
  end

  defp decode_type("DateTime('" <> rest) do
    [timezone] = :binary.split(rest, ["'", ")"], [:global, :trim_all])
    {:datetime, timezone}
  end

  defp decode_type("DateTime64(" <> rest) do
    case :binary.split(rest, [", ", ")", "'"], [:global, :trim_all]) do
      [precision, timezone] ->
        time_unit = round(:math.pow(10, String.to_integer(precision)))
        {:datetime64, time_unit, timezone}

      [precision] ->
        time_unit = round(:math.pow(10, String.to_integer(precision)))
        {:datetime64, time_unit, nil}
    end
  end

  defp decode_type("DateTime" <> _), do: {:datetime, _timezone = nil}
  defp decode_type("Date" <> _), do: :date

  defp decode_type("FixedString(" <> rest) do
    [size] = :binary.split(rest, ")", [:global, :trim])
    string(size: String.to_integer(size))
  end

  defp decode_type("Decimal(" <> rest) do
    [precision, scale] = :binary.split(rest, [", ", ")"], [:global, :trim])
    {scale, _} = Integer.parse(scale)
    precision = String.to_integer(precision)
    decimal(size: decimal_size(precision), scale: scale)
  end

  defp decode_type("LowCardinality(" <> rest) do
    decode_type(rest)
  end

  defp decode_type("Array(" <> rest) do
    {:array, decode_type(rest)}
  end

  defp decode_type("Nullable(" <> rest) do
    {:nullable, decode_type(rest)}
  end

  defp decode_type("Enum8('" <> rest) do
    mapping =
      rest
      |> :binary.split(["' = ", ", '", ")"], [:global, :trim_all])
      |> Enum.chunk_every(2)
      |> Map.new(fn [k, v] -> {String.to_integer(v), k} end)

    {:enum8, mapping}
  end

  defp decode_type("Enum16('" <> rest) do
    mapping =
      rest
      |> :binary.split(["' = ", ", '", ")"], [:global, :trim_all])
      |> Enum.chunk_every(2)
      |> Map.new(fn [k, v] -> {String.to_integer(v), k} end)

    {:enum16, mapping}
  end

  defp decode_type(type) do
    raise ArgumentError, "#{type} type is not supported"
  end

  @compile inline: [decode_string_decode_rows: 5]

  for {pattern, size} <- varints do
    defp decode_string_decode_rows(
           <<unquote(pattern), s::size(unquote(size))-bytes, bin::bytes>>,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_rows(types_rest, bin, [s | row], rows, types)
    end
  end

  @compile inline: [decode_array_decode_rows: 6]
  defp decode_array_decode_rows(<<0, bin::bytes>>, _type, types_rest, row, rows, types) do
    decode_rows(types_rest, bin, [[] | row], rows, types)
  end

  for {pattern, size} <- varints do
    defp decode_array_decode_rows(
           <<unquote(pattern), bin::bytes>>,
           type,
           types_rest,
           row,
           rows,
           types
         ) do
      array_types = List.duplicate(type, unquote(size))
      types_rest = array_types ++ [{:array_over, row} | types_rest]
      decode_rows(types_rest, bin, [], rows, types)
    end
  end

  defp decode_rows([type | types_rest], <<bin::bytes>>, row, rows, types) do
    case type do
      :u8 ->
        <<u, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :u16 ->
        <<u::16-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :u32 ->
        <<u::32-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :u64 ->
        <<u::64-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :u128 ->
        <<u::128-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :u256 ->
        <<u::256-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [u | row], rows, types)

      :i8 ->
        <<i::signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :i16 ->
        <<i::16-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :i32 ->
        <<i::32-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :i64 ->
        <<i::64-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :i128 ->
        <<i::128-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :i256 ->
        <<i::256-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [i | row], rows, types)

      :f32 ->
        case bin do
          <<f::32-little-float, bin::bytes>> ->
            decode_rows(types_rest, bin, [f | row], rows, types)

          <<_nan_or_inf::32, bin::bytes>> ->
            decode_rows(types_rest, bin, [nil | row], rows, types)
        end

      :f64 ->
        case bin do
          <<f::64-little-float, bin::bytes>> ->
            decode_rows(types_rest, bin, [f | row], rows, types)

          <<_nan_or_inf::64, bin::bytes>> ->
            decode_rows(types_rest, bin, [nil | row], rows, types)
        end

      :string ->
        decode_string_decode_rows(bin, types_rest, row, rows, types)

      string(size: size) ->
        <<s::size(size)-bytes, bin::bytes>> = bin
        decode_rows(types_rest, bin, [s | row], rows, types)

      :boolean ->
        case bin do
          <<0, bin::bytes>> -> decode_rows(types_rest, bin, [false | row], rows, types)
          <<1, bin::bytes>> -> decode_rows(types_rest, bin, [true | row], rows, types)
        end

      :uuid ->
        <<u1::64-little, u2::64-little, bin::bytes>> = bin
        uuid = <<u1::64, u2::64>>
        decode_rows(types_rest, bin, [uuid | row], rows, types)

      :date ->
        <<d::16-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Date.add(@epoch_date, d) | row], rows, types)

      :date32 ->
        <<d::32-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Date.add(@epoch_date, d) | row], rows, types)

      {:datetime, timezone} ->
        <<s::32-little, bin::bytes>> = bin

        dt =
          case timezone do
            nil -> NaiveDateTime.add(@epoch_naive_datetime, s)
            "UTC" -> DateTime.from_unix!(s)
            # TODO
            _ -> s |> DateTime.from_unix!() |> DateTime.shift_zone!(timezone)
          end

        decode_rows(types_rest, bin, [dt | row], rows, types)

      decimal(size: size, scale: scale) ->
        <<val::size(size)-little, bin::bytes>> = bin
        sign = if val < 0, do: -1, else: 1
        d = Decimal.new(sign, abs(val), -scale)
        decode_rows(types_rest, bin, [d | row], rows, types)

      {:nullable, type} ->
        case bin do
          <<1, bin::bytes>> -> decode_rows(types_rest, bin, [nil | row], rows, types)
          <<0, bin::bytes>> -> decode_rows([type | types_rest], bin, row, rows, types)
        end

      {:array, type} ->
        decode_array_decode_rows(bin, type, types_rest, row, rows, types)

      {:array_over, original_row} ->
        decode_rows(types_rest, bin, [:lists.reverse(row) | original_row], rows, types)

      {:datetime64, time_unit, timezone} ->
        <<s::64-little-signed, bin::bytes>> = bin

        dt =
          case timezone do
            nil ->
              NaiveDateTime.add(@epoch_naive_datetime, s, time_unit)

            "UTC" ->
              DateTime.from_unix!(s, time_unit)

            _ ->
              s
              |> DateTime.from_unix!(time_unit)
              |> DateTime.shift_zone!(timezone)
          end

        decode_rows(types_rest, bin, [dt | row], rows, types)

      {:enum8, mapping} ->
        <<v, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)

      {:enum16, mapping} ->
        <<v::16-little, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)
    end
  end

  defp decode_rows([], <<>>, row, rows, _types) do
    :lists.reverse([:lists.reverse(row) | rows])
  end

  defp decode_rows([], <<bin::bytes>>, row, rows, types) do
    row = :lists.reverse(row)
    decode_rows(types, bin, [], [row | rows], types)
  end

  # https://clickhouse.com/docs/en/sql-reference/data-types/decimal/
  def decimal_size(precision) when is_integer(precision) do
    cond do
      precision >= 39 -> 256
      precision >= 19 -> 128
      precision >= 10 -> 64
      true -> 32
    end
  end
end
