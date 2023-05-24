defmodule Ch.RowBinary do
  @moduledoc "Helpers for working with ClickHouse [`RowBinary`](https://clickhouse.com/docs/en/sql-reference/formats#rowbinary) format."

  # @compile {:bin_opt_info, true}
  @dialyzer :no_improper_lists

  import Bitwise

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])
  @epoch_utc_datetime DateTime.new!(@epoch_date, ~T[00:00:00])

  @doc false
  def encode_names_and_types(names, types) do
    [encode(:varint, length(names)), encode_many(names, :string), encode_types(types)]
  end

  defp encode_types([type | types]) do
    encoded =
      case type do
        _ when is_binary(type) -> type
        _ -> Ch.Types.encode(type)
      end

    [encode(:string, encoded) | encode_types(types)]
  end

  defp encode_types([] = done), do: done

  @doc """
  Encodes a single row to [`RowBinary`](https://clickhouse.com/docs/en/sql-reference/formats#rowbinary) as iodata.

  Examples:

      iex> encode_row([], [])
      []

      iex> encode_row([1], ["UInt8"])
      [1]

      iex> encode_row([3, "hello"], ["UInt8", "String"])
      [3, [5 | "hello"]]

  """
  def encode_row(row, types) do
    _encode_row(row, encoding_types(types))
  end

  defp _encode_row([el | els], [type | types]), do: [encode(type, el) | _encode_row(els, types)]
  defp _encode_row([] = done, []), do: done

  @doc """
  Encodes multiple rows to [`RowBinary`](https://clickhouse.com/docs/en/sql-reference/formats#rowbinary) as iodata.

  Examples:

      iex> encode_rows([], [])
      []

      iex> encode_rows([[1]], ["UInt8"])
      [1]

      iex> encode_rows([[3, "hello"], [4, "hi"]], ["UInt8", "String"])
      [3, [5 | "hello"], 4, [2 | "hi"]]

  """
  def encode_rows(rows, types) do
    _encode_rows(rows, encoding_types(types))
  end

  @doc false
  def _encode_rows([row | rows], types), do: _encode_rows(row, types, rows, types)
  def _encode_rows([] = done, _types), do: done

  defp _encode_rows([el | els], [t | ts], rows, types) do
    [encode(t, el) | _encode_rows(els, ts, rows, types)]
  end

  defp _encode_rows([], [], rows, types), do: _encode_rows(rows, types)

  @doc false
  def encoding_types([type | types]) do
    [encoding_type(type) | encoding_types(types)]
  end

  def encoding_types([] = done), do: done

  defp encoding_type(type) when is_binary(type) do
    encoding_type(Ch.Types.decode(type))
  end

  defp encoding_type(t)
       when t in [
              :string,
              :binary,
              :boolean,
              :uuid,
              :date,
              :datetime,
              :date32,
              :ipv4,
              :ipv6,
              :point,
              :nothing
            ],
       do: t

  defp encoding_type({:datetime = d, "UTC"}), do: d

  defp encoding_type({:datetime, tz}) do
    raise ArgumentError, "can't encode DateTime with non-UTC timezone: #{inspect(tz)}"
  end

  defp encoding_type({:fixed_string, _len} = t), do: t

  for size <- [8, 16, 32, 64, 128, 256] do
    defp encoding_type(unquote(:"u#{size}") = u), do: u
    defp encoding_type(unquote(:"i#{size}") = i), do: i
  end

  for size <- [32, 64] do
    defp encoding_type(unquote(:"f#{size}") = f), do: f
  end

  defp encoding_type({:array = a, t}), do: {a, encoding_type(t)}

  defp encoding_type({:tuple = t, ts}) do
    {t, Enum.map(ts, &encoding_type/1)}
  end

  defp encoding_type({:map = m, kt, vt}) do
    {m, encoding_type(kt), encoding_type(vt)}
  end

  defp encoding_type({:nullable = n, t}), do: {n, encoding_type(t)}
  defp encoding_type({:low_cardinality, t}), do: encoding_type(t)

  defp encoding_type({:decimal, p, s}) do
    case decimal_size(p) do
      32 -> {:decimal32, s}
      64 -> {:decimal64, s}
      128 -> {:decimal128, s}
      256 -> {:decimal256, s}
    end
  end

  defp encoding_type({d, _scale} = t)
       when d in [:decimal32, :decimal64, :decimal128, :decimal256],
       do: t

  defp encoding_type({:datetime64 = t, p}), do: {t, time_unit(p)}

  defp encoding_type({:datetime64 = t, p, "UTC"}), do: {t, time_unit(p)}

  defp encoding_type({:datetime64, _, tz}) do
    raise ArgumentError, "can't encode DateTime64 with non-UTC timezone: #{inspect(tz)}"
  end

  defp encoding_type({e, mappings}) when e in [:enum8, :enum16] do
    {e, Map.new(mappings)}
  end

  defp encoding_type({:simple_aggregate_function, _f, t}), do: encoding_type(t)

  defp encoding_type(:ring), do: {:array, :point}
  defp encoding_type(:polygon), do: {:array, {:array, :point}}
  defp encoding_type(:multipolygon), do: {:array, {:array, {:array, :point}}}

  defp encoding_type(type) do
    raise ArgumentError, "unsupported type for encoding: #{inspect(type)}"
  end

  @doc false
  def encode(type, value)

  def encode(:varint, i) when is_integer(i) and i < 128, do: i
  def encode(:varint, i) when is_integer(i), do: encode_varint_cont(i)

  def encode(type, str) when type in [:string, :binary] do
    case str do
      _ when is_binary(str) -> [encode(:varint, byte_size(str)) | str]
      _ when is_list(str) -> [encode(:varint, IO.iodata_length(str)) | str]
      nil -> 0
    end
  end

  def encode({:fixed_string, size}, str) when byte_size(str) == size do
    str
  end

  def encode({:fixed_string, size}, str) when byte_size(str) < size do
    to_pad = size - byte_size(str)
    [str | <<0::size(to_pad * 8)>>]
  end

  def encode({:fixed_string, size}, nil), do: <<0::size(size * 8)>>

  def encode(:u8, u) when is_integer(u), do: u
  def encode(:u8, nil), do: 0

  def encode(:i8, i) when is_integer(i) and i >= 0, do: i
  def encode(:i8, i) when is_integer(i), do: <<i::signed>>
  def encode(:i8, nil), do: 0

  for size <- [16, 32, 64, 128, 256] do
    def encode(unquote(:"u#{size}"), u) when is_integer(u) do
      <<u::unquote(size)-little>>
    end

    def encode(unquote(:"i#{size}"), i) when is_integer(i) do
      <<i::unquote(size)-little-signed>>
    end

    def encode(unquote(:"u#{size}"), nil), do: <<0::unquote(size)>>
    def encode(unquote(:"i#{size}"), nil), do: <<0::unquote(size)>>
  end

  for size <- [32, 64] do
    type = :"f#{size}"

    def encode(unquote(type), f) when is_number(f) do
      <<f::unquote(size)-little-signed-float>>
    end

    def encode(unquote(type), nil), do: <<0::unquote(size)>>
  end

  def encode({:decimal, precision, scale}, decimal) do
    type =
      case decimal_size(precision) do
        32 -> :decimal32
        64 -> :decimal64
        128 -> :decimal128
        256 -> :decimal256
      end

    encode({type, scale}, decimal)
  end

  for size <- [32, 64, 128, 256] do
    type = :"decimal#{size}"

    def encode({unquote(type), scale} = t, %Decimal{sign: sign, coef: coef, exp: exp} = d) do
      cond do
        scale == -exp ->
          i = sign * coef
          <<i::unquote(size)-little>>

        exp >= 0 ->
          i = sign * coef * round(:math.pow(10, exp + scale))
          <<i::unquote(size)-little>>

        true ->
          encode(t, Decimal.round(d, scale))
      end
    end

    def encode({unquote(type), _scale}, nil), do: <<0::unquote(size)>>
  end

  def encode(:boolean, true), do: 1
  def encode(:boolean, false), do: 0
  def encode(:boolean, nil), do: 0

  def encode({:array, type}, [_ | _] = l) do
    [encode(:varint, length(l)) | encode_many(l, type)]
  end

  def encode({:array, _type}, []), do: 0
  def encode({:array, _type}, nil), do: 0

  def encode({:map, k, v}, [_ | _] = m) do
    [encode(:varint, length(m)) | encode_many_kv(m, k, v)]
  end

  def encode({:map, _k, _v} = t, m) when is_map(m), do: encode(t, Map.to_list(m))
  def encode({:map, _k, _v}, []), do: 0
  def encode({:map, _k, _v}, nil), do: 0

  def encode({:tuple, _types} = t, v) when is_tuple(v) do
    encode(t, Tuple.to_list(v))
  end

  def encode({:tuple, types}, values) when is_list(types) and is_list(values) do
    encode_row(values, types)
  end

  def encode({:tuple, types}, nil) when is_list(types) do
    Enum.map(types, fn type -> encode(type, nil) end)
  end

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:datetime, %DateTime{time_zone: "Etc/UTC"} = datetime) do
    <<DateTime.to_unix(datetime, :second)::32-little>>
  end

  def encode(:datetime, %DateTime{} = datetime) do
    raise ArgumentError, "non-UTC timezones are not supported for encoding: #{datetime}"
  end

  def encode(:datetime, nil), do: <<0::32>>

  def encode({:datetime64, time_unit}, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime, time_unit)::64-little-signed>>
  end

  def encode({:datetime64, time_unit}, %DateTime{time_zone: "Etc/UTC"} = datetime) do
    <<DateTime.diff(datetime, @epoch_utc_datetime, time_unit)::64-little-signed>>
  end

  def encode({:datetime64, _precision}, %DateTime{} = datetime) do
    raise ArgumentError, "non-UTC timezones are not supported for encoding: #{datetime}"
  end

  def encode({:datetime64, _precision}, nil), do: <<0::64>>

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end

  def encode(:date, nil), do: <<0::16>>

  def encode(:date32, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::32-little-signed>>
  end

  def encode(:date32, nil), do: <<0::32>>

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

  def encode(:uuid, nil), do: <<0::128>>

  def encode(:ipv4, {a, b, c, d}), do: [d, c, b, a]
  def encode(:ipv4, nil), do: <<0::32>>

  def encode(:ipv6, {b1, b2, b3, b4, b5, b6, b7, b8}) do
    <<b1::16, b2::16, b3::16, b4::16, b5::16, b6::16, b7::16, b8::16>>
  end

  def encode(:ipv6, <<_::128>> = encoded), do: encoded
  def encode(:ipv6, nil), do: <<0::128>>

  def encode(:point, {x, y}), do: [encode(:f64, x) | encode(:f64, y)]
  def encode(:point, nil), do: <<0::128>>
  def encode(:ring, points), do: encode({:array, :point}, points)
  def encode(:polygon, rings), do: encode({:array, :ring}, rings)
  def encode(:multipolygon, polygons), do: encode({:array, :polygon}, polygons)

  # TODO enum8 and enum16 nil
  for size <- [8, 16] do
    enum_t = :"enum#{size}"
    int_t = :"i#{size}"

    def encode({unquote(enum_t), mapping}, e) do
      i =
        case e do
          _ when is_integer(e) ->
            e

          _ when is_binary(e) ->
            case Map.fetch(mapping, e) do
              {:ok, res} ->
                res

              :error ->
                raise ArgumentError,
                      "enum value #{inspect(e)} not found in mapping: #{inspect(mapping)}"
            end
        end

      encode(unquote(int_t), i)
    end
  end

  def encode({:nullable, _type}, nil), do: 1

  def encode({:nullable, type}, value) do
    case encode(type, value) do
      e when is_list(e) or is_binary(e) -> [0 | e]
      e -> [0, e]
    end
  end

  defp encode_varint_cont(i) when i < 128, do: <<i>>

  defp encode_varint_cont(i) do
    [(i &&& 0b0111_1111) ||| 0b1000_0000 | encode_varint_cont(i >>> 7)]
  end

  defp encode_many([el | rest], type), do: [encode(type, el) | encode_many(rest, type)]
  defp encode_many([] = done, _type), do: done

  defp encode_many_kv([{key, value} | rest], key_type, value_type) do
    [
      encode(key_type, key),
      encode(value_type, value)
      | encode_many_kv(rest, key_type, value_type)
    ]
  end

  defp encode_many_kv([] = done, _key_type, _value_type), do: done

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

  @doc """
  Decodes [`RowBinaryWithNamesAndTypes`](https://clickhouse.com/docs/en/sql-reference/formats#rowbinarywithnamesandtypes) into rows.

  Example:

      iex> decode_rows(<<1, 3, "1+1"::bytes, 5, "UInt8"::bytes, 2>>)
      [[2]]

  """
  def decode_rows(row_binary_with_names_and_types)
  def decode_rows(<<cols, rest::bytes>>), do: skip_names(rest, cols, cols)
  def decode_rows(<<>>), do: []

  @doc """
  Decodes [`RowBinary`](https://clickhouse.com/docs/en/sql-reference/formats#rowbinary) into rows.

  Example:

      iex> decode_rows(<<1>>, ["UInt8"])
      [[1]]

  """
  def decode_rows(row_binary, types)
  def decode_rows(<<>>, _types), do: []

  def decode_rows(<<data::bytes>>, types) do
    types = decoding_types(types)
    decode_rows(types, data, [], [], types)
  end

  @doc false
  def decoding_types([type | types]) do
    [decoding_type(type) | types]
  end

  def decoding_types([] = done), do: done

  defp decoding_type(t) when is_binary(t) do
    decoding_type(Ch.Types.decode(t))
  end

  defp decoding_type(t)
       when t in [
              :string,
              :binary,
              :boolean,
              :uuid,
              :date,
              :date32,
              :ipv4,
              :ipv6,
              :point,
              :nothing
            ],
       do: t

  defp decoding_type({:datetime, _tz} = t), do: t
  defp decoding_type({:fixed_string, _len} = t), do: t

  for size <- [8, 16, 32, 64, 128, 256] do
    defp decoding_type(unquote(:"u#{size}") = u), do: u
    defp decoding_type(unquote(:"i#{size}") = i), do: i
  end

  for size <- [32, 64] do
    defp decoding_type(unquote(:"f#{size}") = f), do: f
  end

  defp decoding_type(:datetime = t), do: {t, _tz = nil}

  defp decoding_type({:array = a, t}), do: {a, decoding_type(t)}

  defp decoding_type({:tuple = t, ts}) do
    {t, Enum.map(ts, &decoding_type/1)}
  end

  defp decoding_type({:map = m, kt, vt}) do
    {m, decoding_type(kt), decoding_type(vt)}
  end

  defp decoding_type({:nullable = n, t}), do: {n, decoding_type(t)}
  defp decoding_type({:low_cardinality, t}), do: decoding_type(t)

  defp decoding_type({:decimal = t, p, s}), do: {t, decimal_size(p), s}
  defp decoding_type({:decimal32, s}), do: {:decimal, 32, s}
  defp decoding_type({:decimal64, s}), do: {:decimal, 64, s}
  defp decoding_type({:decimal128, s}), do: {:decimal, 128, s}
  defp decoding_type({:decimal256, s}), do: {:decimal, 256, s}

  defp decoding_type({:datetime64 = t, p}), do: {t, time_unit(p), _tz = nil}
  defp decoding_type({:datetime64 = t, p, tz}), do: {t, time_unit(p), tz}

  defp decoding_type({e, mappings}) when e in [:enum8, :enum16] do
    {e, Map.new(mappings, fn {k, v} -> {v, k} end)}
  end

  defp decoding_type({:simple_aggregate_function, _f, t}), do: decoding_type(t)

  defp decoding_type(:ring), do: {:array, :point}
  defp decoding_type(:polygon), do: {:array, {:array, :point}}
  defp decoding_type(:multipolygon), do: {:array, {:array, {:array, :point}}}

  defp decoding_type(type) do
    raise ArgumentError, "unsupported type for decoding: #{inspect(type)}"
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
    [decoding_type(Ch.Types.decode(type)) | decode_types(types)]
  end

  def decode_types([] = done), do: done

  @compile inline: [decode_string_decode_rows: 5]

  for {pattern, size} <- varints do
    defp decode_string_decode_rows(
           <<unquote(pattern), s::size(unquote(size))-bytes, bin::bytes>>,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_rows(types_rest, bin, [to_utf8(s) | row], rows, types)
    end
  end

  @doc false
  def to_utf8(str) do
    utf8 = to_utf8(str, 0, 0, str, [])
    IO.iodata_to_binary(utf8)
  end

  @dialyzer {:no_improper_lists, to_utf8: 5, to_utf8_escape: 5}

  defp to_utf8(<<valid::utf8, rest::bytes>>, from, len, original, acc) do
    to_utf8(rest, from, len + utf8_size(valid), original, acc)
  end

  defp to_utf8(<<_invalid, rest::bytes>>, from, len, original, acc) do
    acc = [acc | binary_part(original, from, len)]
    to_utf8_escape(rest, from + len, 1, original, acc)
  end

  defp to_utf8(<<>>, from, len, original, acc) do
    [acc | binary_part(original, from, len)]
  end

  defp to_utf8_escape(<<valid::utf8, rest::bytes>>, from, len, original, acc) do
    acc = [acc | "�"]
    to_utf8(rest, from + len, utf8_size(valid), original, acc)
  end

  defp to_utf8_escape(<<_invalid, rest::bytes>>, from, len, original, acc) do
    to_utf8_escape(rest, from, len + 1, original, acc)
  end

  defp to_utf8_escape(<<>>, _from, _len, _original, acc) do
    [acc | "�"]
  end

  # UTF-8 encodes code points in one to four bytes
  @compile inline: [utf8_size: 1]
  defp utf8_size(codepoint) when codepoint <= 0x7F, do: 1
  defp utf8_size(codepoint) when codepoint <= 0x7FF, do: 2
  defp utf8_size(codepoint) when codepoint <= 0xFFFF, do: 3
  defp utf8_size(codepoint) when codepoint <= 0x10FFFF, do: 4

  @compile inline: [decode_binary_decode_rows: 5]

  for {pattern, size} <- varints do
    defp decode_binary_decode_rows(
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

  @compile inline: [decode_map_decode_rows: 7]
  defp decode_map_decode_rows(
         <<0, bin::bytes>>,
         _key_type,
         _value_type,
         types_rest,
         row,
         rows,
         types
       ) do
    decode_rows(types_rest, bin, [%{} | row], rows, types)
  end

  for {pattern, size} <- varints do
    defp decode_map_decode_rows(
           <<unquote(pattern), bin::bytes>>,
           key_type,
           value_type,
           types_rest,
           row,
           rows,
           types
         ) do
      types_rest =
        map_types(unquote(size), key_type, value_type) ++ [{:map_over, row} | types_rest]

      decode_rows(types_rest, bin, [], rows, types)
    end
  end

  defp map_types(count, key_type, value_type) when count > 0 do
    [key_type, value_type | map_types(count - 1, key_type, value_type)]
  end

  defp map_types(0, _key_type, _value_types), do: []

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

      :binary ->
        decode_binary_decode_rows(bin, types_rest, row, rows, types)

      # TODO utf8?
      {:fixed_string, size} ->
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
            _ -> s |> DateTime.from_unix!() |> DateTime.shift_zone!(timezone)
          end

        decode_rows(types_rest, bin, [dt | row], rows, types)

      {:decimal, size, scale} ->
        <<val::size(size)-little-signed, bin::bytes>> = bin
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

      {:map, key_type, value_type} ->
        decode_map_decode_rows(bin, key_type, value_type, types_rest, row, rows, types)

      {:map_over, original_row} ->
        map = row |> Enum.chunk_every(2) |> Enum.map(fn [v, k] -> {k, v} end) |> Map.new()
        decode_rows(types_rest, bin, [map | original_row], rows, types)

      {:tuple, tuple_types} ->
        decode_rows(tuple_types ++ [{:tuple_over, row} | types_rest], bin, [], rows, types)

      {:tuple_over, original_row} ->
        tuple = row |> :lists.reverse() |> List.to_tuple()
        decode_rows(types_rest, bin, [tuple | original_row], rows, types)

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
        <<v::signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)

      {:enum16, mapping} ->
        <<v::16-little-signed, bin::bytes>> = bin
        decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)

      :ipv4 ->
        <<b4, b3, b2, b1, bin::bytes>> = bin
        decode_rows(types_rest, bin, [{b1, b2, b3, b4} | row], rows, types)

      :ipv6 ->
        <<b1::16, b2::16, b3::16, b4::16, b5::16, b6::16, b7::16, b8::16, bin::bytes>> = bin
        decode_rows(types_rest, bin, [{b1, b2, b3, b4, b5, b6, b7, b8} | row], rows, types)

      :point ->
        <<x::64-little-float, y::64-little-float, bin::bytes>> = bin
        decode_rows(types_rest, bin, [{x, y} | row], rows, types)
    end
  end

  defp decode_rows([], <<>>, row, rows, _types) do
    :lists.reverse([:lists.reverse(row) | rows])
  end

  defp decode_rows([], <<bin::bytes>>, row, rows, types) do
    row = :lists.reverse(row)
    decode_rows(types, bin, [], [row | rows], types)
  end

  @compile inline: [decimal_size: 1]
  # https://clickhouse.com/docs/en/sql-reference/data-types/decimal/
  defp decimal_size(precision) when is_integer(precision) do
    cond do
      precision >= 39 -> 256
      precision >= 19 -> 128
      precision >= 10 -> 64
      true -> 32
    end
  end

  @compile inline: [time_unit: 1]
  for precision <- 0..9 do
    time_unit = round(:math.pow(10, precision))
    defp time_unit(unquote(precision)), do: unquote(time_unit)
  end
end
