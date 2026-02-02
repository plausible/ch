defmodule Ch.RowBinary do
  @moduledoc "Helpers for working with ClickHouse [RowBinary](https://clickhouse.com/docs/en/interfaces/formats/RowBinary) format."

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
  Encodes a single row to [RowBinary](https://clickhouse.com/docs/en/interfaces/formats/RowBinary) as iodata.

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
  Encodes multiple rows to [RowBinary](https://clickhouse.com/docs/en/interfaces/formats/RowBinary) as iodata.

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
              :json,
              :dynamic,
              :boolean,
              :uuid,
              :date,
              :datetime,
              :date32,
              :time,
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

  defp encoding_type({:variant = v, ts}) do
    {v, Enum.map(ts, &encoding_type/1)}
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

  defp encoding_type({:time64 = t, p}), do: {t, time_unit(p)}

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

  def encode(:json, json) do
    # assuming it can be sent as text and not "native" binary JSON
    # i.e. assumes `settings: [input_format_binary_read_json_as_string: 1]`
    # TODO
    encode(:string, Jason.encode_to_iodata!(json))
  end

  def encode({:fixed_string, size}, str) when byte_size(str) == size do
    str
  end

  def encode({:fixed_string, size}, str) when byte_size(str) < size do
    to_pad = size - byte_size(str)
    [str | <<0::size(to_pad * 8)>>]
  end

  def encode({:fixed_string, size}, nil), do: <<0::size(size * 8)>>

  # UInt8 — [0 : 255]
  def encode(:u8, u) when is_integer(u) and u >= 0 and u <= 255, do: u
  def encode(:u8, nil), do: 0

  def encode(:u8, term) do
    raise ArgumentError, "invalid UInt8: #{inspect(term)}"
  end

  # Int8 — [-128 : 127]
  def encode(:i8, i) when is_integer(i) and i >= 0 and i <= 127, do: i
  def encode(:i8, i) when is_integer(i) and i < 0 and i >= -128, do: <<i::signed>>
  def encode(:i8, nil), do: 0

  def encode(:i8, term) do
    raise ArgumentError, "invalid Int8: #{inspect(term)}"
  end

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

  def encode({:variant, _types}, nil), do: 255

  def encode({:variant, types}, value) do
    try_encode_variant(types, 0, value)
  end

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:datetime, %DateTime{time_zone: "Etc/UTC"} = datetime) do
    <<DateTime.to_unix(datetime, :second)::32-little>>
  end

  def encode(:datetime, %DateTime{} = datetime) do
    encode(:datetime, DateTime.shift_zone!(datetime, "Etc/UTC"))
  end

  def encode(:datetime, nil), do: <<0::32>>

  def encode({:datetime64, time_unit}, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime, time_unit)::64-little-signed>>
  end

  def encode({:datetime64, time_unit}, %DateTime{time_zone: "Etc/UTC"} = datetime) do
    <<DateTime.diff(datetime, @epoch_utc_datetime, time_unit)::64-little-signed>>
  end

  def encode({:datetime64, _time_unit}, %DateTime{} = datetime) do
    raise ArgumentError, "non-UTC timezones are not supported for encoding: #{datetime}"
  end

  def encode({:datetime64, _time_unit}, nil), do: <<0::64>>

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end

  def encode(:date, nil), do: <<0::16>>

  def encode(:date32, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::32-little-signed>>
  end

  def encode(:date32, nil), do: <<0::32>>

  def encode(:time, %Time{} = time) do
    {s, _micros} = Time.to_seconds_after_midnight(time)
    <<s::32-little-signed>>
  end

  def encode(:time, nil), do: <<0::32>>

  def encode({:time64, time_unit}, %Time{} = time) do
    {s, micros} = Time.to_seconds_after_midnight(time)

    micros_as_ticks =
      cond do
        time_unit < 1_000_000 -> div(micros, time_unit)
        time_unit == 1_000_000 -> micros
        true -> micros * div(time_unit, 1_000_000)
      end

    ticks = s * time_unit + micros_as_ticks
    <<ticks::64-little-signed>>
  end

  def encode({:time64, _time_unit}, nil), do: <<0::64>>

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

  # TODO
  def encode(:dynamic, value) do
    case value do
      _ when is_binary(value) -> [0x15 | encode(:string, value)]
      _ when is_integer(value) and value >= 0 -> [0x04 | encode(:u64, value)]
      _ when is_integer(value) -> [0x0A | encode(:i64, value)]
      _ when is_float(value) -> [0x0E | encode(:f64, value)]
      %Date{} -> [0x0F | encode(:date, value)]
      %DateTime{} -> [0x11 | encode(:datetime, value)]
      %NaiveDateTime{} -> [0x11 | encode(:datetime, value)]
      %{} -> [0x30, 0x00, 0x80, 0x08, 0x20, 0x00, 0x00, 0x00 | encode(:json, value)]
      [] -> [0x1E, 0x00]
    end
  end

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

  # TODO find a better way than try/rescue
  defp try_encode_variant([type | types], idx, value) do
    try do
      encode(type, value)
    else
      encoded -> [idx | encoded]
    rescue
      _e -> try_encode_variant(types, idx + 1, value)
    end
  end

  defp try_encode_variant([], _idx, value) do
    raise ArgumentError, "no matching type found for encoding #{inspect(value)} as Variant"
  end

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

  @doc false
  @spec decode_header(binary()) ::
          {:ok, names :: [String.t()], types :: [term], rest :: binary} | :more
  def decode_header(row_binary_with_names_and_types)

  for {pattern, value} <- varints do
    def decode_header(<<unquote(pattern), rest::bytes>>) do
      decode_header_names(rest, unquote(value), unquote(value), _acc = [])
    end
  end

  def decode_header(<<_bin::bytes>>) do
    :more
  end

  defp decode_header_names(<<rest::bytes>>, 0, count, names) do
    decode_header_types(rest, count, _acc = [], :lists.reverse(names))
  end

  for {pattern, value} <- varints do
    defp decode_header_names(
           <<unquote(pattern), name::size(unquote(value))-bytes, rest::bytes>>,
           left,
           count,
           acc
         ) do
      decode_header_names(rest, left - 1, count, [name | acc])
    end
  end

  defp decode_header_names(<<_bin::bytes>>, _left, _count, _acc) do
    :more
  end

  defp decode_header_types(<<rest::bytes>>, 0, types, names) do
    {:ok, names, decoding_types_reverse(types), rest}
  end

  for {pattern, value} <- varints do
    defp decode_header_types(
           <<unquote(pattern), type::size(unquote(value))-bytes, rest::bytes>>,
           count,
           acc,
           names
         ) do
      decode_header_types(rest, count - 1, [type | acc], names)
    end
  end

  defp decode_header_types(<<_bin::bytes>>, _count, _acc, _names) do
    :more
  end

  @doc """
  Decodes [RowBinaryWithNamesAndTypes](https://clickhouse.com/docs/en/interfaces/formats/RowBinaryWithNamesAndTypes) into rows.

  Example:

      iex> decode_rows(<<1, 3, "1+1"::bytes, 5, "UInt8"::bytes, 2>>)
      [[2]]

  """
  def decode_rows(row_binary_with_names_and_types)
  def decode_rows(<<>>), do: []

  for {pattern, value} <- varints do
    def decode_rows(<<unquote(pattern), rest::bytes>>) do
      skip_names(rest, unquote(value), unquote(value))
    end
  end

  @doc """
  Same as `decode_rows/1` but the first element is a list of column names.

  Example:

      iex> decode_names_and_rows(<<1, 3, "1+1"::bytes, 5, "UInt8"::bytes, 2>>)
      [["1+1"], [2]]

  """
  def decode_names_and_rows(row_binary_with_names_and_types)

  for {pattern, value} <- varints do
    def decode_names_and_rows(<<unquote(pattern), rest::bytes>>) do
      decode_names(rest, unquote(value), unquote(value), _acc = [])
    end
  end

  @doc """
  Decodes [RowBinary](https://clickhouse.com/docs/en/interfaces/formats/RowBinary) into rows.

  Example:

      iex> decode_rows(<<1>>, ["UInt8"])
      [[1]]

  """
  def decode_rows(row_binary, types)
  def decode_rows(<<>>, _types), do: []

  def decode_rows(<<data::bytes>>, types) do
    decode_rows!(data, decoding_types(types))
  end

  defp decode_rows!(data, types) do
    {rows, remaining_data, state} = decode_rows(types, data, [], [], types)

    case state do
      nil ->
        rows

      {:cont, types_rest, row} ->
        raise ArgumentError, """
        incomplete RowBinary data: ran out of bytes while decoding

        Expected to decode: #{inspect(types_rest)}
        Remaining bytes: #{byte_size(remaining_data)} bytes
        Partial row: #{inspect(row)}
        Completed rows: #{length(rows)}
        """
    end
  end

  @doc false
  def decode_rows_continue(<<data::bytes>>, types, state) do
    case state do
      {:cont, types_rest, row} -> decode_rows(types_rest, data, row, [], types)
      nil -> decode_rows(types, data, [], [], types)
    end
  end

  @doc false
  def decoding_types([type | types]) do
    [decoding_type(type) | decoding_types(types)]
  end

  def decoding_types([] = done), do: done

  defp decoding_types_reverse(types), do: decoding_types_reverse(types, [])

  defp decoding_types_reverse([type | types], acc) do
    decoding_types_reverse(types, [decoding_type(type) | acc])
  end

  defp decoding_types_reverse([], acc), do: acc

  defp decoding_type(t) when is_binary(t) do
    decoding_type(Ch.Types.decode(t))
  end

  defp decoding_type(t)
       when t in [
              :string,
              :binary,
              :json,
              :dynamic,
              :boolean,
              :uuid,
              :date,
              :date32,
              :time,
              :time64,
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

  defp decoding_type({:variant = v, ts}) do
    {v, Enum.map(ts, &decoding_type/1)}
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

  defp decoding_type({:time64 = t, p}), do: {t, time_unit(p)}

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

  for {pattern, value} <- varints do
    defp skip_names(<<unquote(pattern), _::size(unquote(value))-bytes, rest::bytes>>, left, count) do
      skip_names(rest, left - 1, count)
    end
  end

  defp decode_names(<<rest::bytes>>, 0, count, names) do
    [:lists.reverse(names) | decode_types(rest, count, _acc = [])]
  end

  for {pattern, value} <- varints do
    defp decode_names(
           <<unquote(pattern), name::size(unquote(value))-bytes, rest::bytes>>,
           left,
           count,
           acc
         ) do
      decode_names(rest, left - 1, count, [name | acc])
    end
  end

  defp decode_types(<<>>, 0, _types), do: []

  defp decode_types(<<rest::bytes>>, 0, types) do
    decode_rows!(rest, decoding_types_reverse(types))
  end

  for {pattern, value} <- varints do
    defp decode_types(
           <<unquote(pattern), type::size(unquote(value))-bytes, rest::bytes>>,
           count,
           acc
         ) do
      decode_types(rest, count - 1, [type | acc])
    end
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
      decode_rows(types_rest, bin, [to_utf8(s) | row], rows, types)
    end
  end

  defp decode_string_decode_rows(<<bin::bytes>>, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [:string | types_rest], row)
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

  @compile inline: [decode_string_json_decode_rows: 5]

  for {pattern, size} <- varints do
    defp decode_string_json_decode_rows(
           <<unquote(pattern), s::size(unquote(size))-bytes, bin::bytes>>,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_rows(types_rest, bin, [Jason.decode!(s) | row], rows, types)
    end
  end

  defp decode_string_json_decode_rows(<<bin::bytes>>, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [:json | types_rest], row)
  end

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

  defp decode_binary_decode_rows(<<bin::bytes>>, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [:binary | types_rest], row)
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

  defp decode_array_decode_rows(<<bin::bytes>>, type, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [{:array, type} | types_rest], row)
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

  defp decode_map_decode_rows(<<bin::bytes>>, key_type, value_type, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [{:map, key_type, value_type} | types_rest], row)
  end

  defp map_types(count, key_type, value_type) when count > 0 do
    [key_type, value_type | map_types(count - 1, key_type, value_type)]
  end

  defp map_types(0, _key_type, _value_types), do: []

  # https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
  dynamic_types = [
    nothing: 0x00,
    u8: 0x01,
    u16: 0x02,
    u32: 0x03,
    u64: 0x04,
    u128: 0x05,
    u256: 0x06,
    i8: 0x07,
    i16: 0x08,
    i32: 0x09,
    i64: 0x0A,
    i128: 0x0B,
    i256: 0x0C,
    f32: 0x0D,
    f64: 0x0E,
    date: 0x0F,
    date32: 0x10,
    string: 0x15,
    uuid: 0x1D,
    ipv4: 0x28,
    ipv6: 0x29,
    boolean: 0x2D
  ]

  # TODO compile inline?

  for {type, code} <- dynamic_types do
    defp decode_dynamic(
           <<unquote(code), rest::bytes>>,
           dynamic,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_dynamic_continue(rest, [unquote(type) | dynamic], types_rest, row, rows, types)
    end
  end

  # DateTime 0x11
  defp decode_dynamic(<<0x11, rest::bytes>>, dynamic, types_rest, row, rows, types) do
    decode_dynamic_continue(rest, [{:datetime, nil} | dynamic], types_rest, row, rows, types)
  end

  # DateTime(time_zone) 0x12 <var_uint_time_zone_name_size><time_zone_name_data>
  for {pattern, size} <- varints do
    defp decode_dynamic(
           <<0x12, unquote(pattern), tz::size(unquote(size))-bytes, rest::bytes>>,
           dynamic,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_dynamic_continue(rest, [{:datetime, tz} | dynamic], types_rest, row, rows, types)
    end
  end

  # DateTime64(P) 0x13 <uint8_precision>
  defp decode_dynamic(
         <<0x13, precision, rest::bytes>>,
         dynamic,
         types_rest,
         row,
         rows,
         types
       ) do
    decode_dynamic_continue(
      rest,
      [decoding_type({:datetime64, precision}) | dynamic],
      types_rest,
      row,
      rows,
      types
    )
  end

  # DateTime64(P, time_zone) 0x14 <uint8_precision><var_uint_time_zone_name_size><time_zone_name_data>
  for {pattern, size} <- varints do
    defp decode_dynamic(
           <<0x14, precision, unquote(pattern), tz::size(unquote(size))-bytes, rest::bytes>>,
           dynamic,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_dynamic_continue(
        rest,
        [decoding_type({:datetime64, precision, tz}) | dynamic],
        types_rest,
        row,
        rows,
        types
      )
    end
  end

  # FixedString(N) 0x16 <var_uint_size>
  for {pattern, size} <- varints do
    defp decode_dynamic(
           <<0x16, unquote(pattern), rest::bytes>>,
           dynamic,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_dynamic_continue(
        rest,
        [{:fixed_string, unquote(size)} | dynamic],
        types_rest,
        row,
        rows,
        types
      )
    end
  end

  # Decimal32(P, S) 0x19 <uint8_precision><uint8_scale>
  # Decimal64(P, S) 0x1A <uint8_precision><uint8_scale>
  # Decimal128(P, S) 0x1B <uint8_precision><uint8_scale>
  # Decimal256(P, S) 0x1C <uint8_precision><uint8_scale>
  for {code, size} <- [{0x19, 32}, {0x1A, 64}, {0x1B, 128}, {0x1C, 256}] do
    defp decode_dynamic(
           <<unquote(code), _precision, scale, rest::bytes>>,
           dynamic,
           types_rest,
           row,
           rows,
           types
         ) do
      decode_dynamic_continue(
        rest,
        [{:decimal, unquote(size), scale} | dynamic],
        types_rest,
        row,
        rows,
        types
      )
    end
  end

  # Array(T) 0x1E <nested_type_encoding>
  defp decode_dynamic(<<0x1E, rest::bytes>>, dynamic, types_rest, row, rows, types) do
    decode_dynamic_continue(rest, [:array | dynamic], types_rest, row, rows, types)
  end

  # Nullable(T)	0x23 <nested_type_encoding>
  defp decode_dynamic(<<0x23, rest::bytes>>, dynamic, types_rest, row, rows, types) do
    decode_dynamic_continue(rest, [:nullable | dynamic], types_rest, row, rows, types)
  end

  # LowCardinality(T) 0x26 <nested_type_encoding>
  defp decode_dynamic(<<0x26, rest::bytes>>, dynamic, types_rest, row, rows, types) do
    decode_dynamic_continue(rest, [:low_cardinality | dynamic], types_rest, row, rows, types)
  end

  # JSON(max_dynamic_paths=N, max_dynamic_types=M, path Type, SKIP skip_path, SKIP REGEXP skip_path_regexp) 0x30<uint8_serialization_version><var_int_max_dynamic_paths><uint8_max_dynamic_types><var_uint_number_of_typed_paths><var_uint_path_name_size_1><path_name_data_1><encoded_type_1>...<var_uint_number_of_skip_paths><var_uint_skip_path_size_1><skip_path_data_1>...<var_uint_number_of_skip_path_regexps><var_uint_skip_path_regexp_size_1><skip_path_data_regexp_1>...
  defp decode_dynamic(<<0x30, rest::bytes>>, dynamic, types_rest, row, rows, types) do
    # Assert uint8_serialization_version to be 0
    <<0x00, rest::bytes>> = rest

    # Skip var_int_max_dynamic_paths
    {_paths, rest} = read_varint(rest)

    # Skip uint8_max_dynamic_types
    <<_val, rest::bytes>> = rest

    # Read var_uint_number_of_typed_paths
    {typed_paths, rest} = read_varint(rest)

    # Skip `typed_paths` typed paths
    rest =
      Enum.reduce(1..typed_paths//1, rest, fn _, rest ->
        {count, rest} = read_varint(rest)
        <<_discard::size(count)-bytes, rest::bytes>> = rest
        skip_type(rest)
      end)

    # Read var_uint_number_of_skip_paths
    {skip_paths, rest} = read_varint(rest)

    # Skip `skip_paths` skipped paths
    rest =
      Enum.reduce(1..skip_paths//1, rest, fn _, rest ->
        {count, rest} = read_varint(rest)
        <<_discard::size(count)-bytes, rest::bytes>> = rest
        rest
      end)

    # Read var_uint_number_of_skip_path_regexps
    {skip_path_regexes, rest} = read_varint(rest)

    # Skip `skip_path_regexes` skipped paths regex
    rest =
      Enum.reduce(1..skip_path_regexes//1, rest, fn _, rest ->
        {count, rest} = read_varint(rest)
        <<_discard::size(count)-bytes, rest::bytes>> = rest
        rest
      end)

    decode_dynamic_continue(rest, [:json | dynamic], types_rest, row, rows, types)
  end

  for {pattern, value} <- varints do
    defp read_varint(<<unquote(pattern), rest::bytes>>), do: {unquote(value), rest}
  end

  other_dynamic_types = [
    datetime: 0x11,
    set: 0x21,
    bfloat16: 0x31,
    time: 0x32
  ]

  # Consume a type header from binary input, returning the rest.
  # TODO: Only supports single-byte type headers for now.
  def skip_type(<<type, rest::bytes>>)
      when type in unquote(Keyword.values(dynamic_types ++ other_dynamic_types)), do: rest

  def skip_type(<<type, _::bytes>>) do
    raise ArgumentError,
          "Unsupported type definition (starting with 0x#{Base.encode16(<<type>>)}) while decoding dynamic JSON. Only single-byte type identifiers are currently supported."
  end

  # TODO
  # Enum8	0x17 <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int8_value_1>...<var_uint_name_size_N><name_data_N><int8_value_N>
  # Enum16	0x18 <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><int16_little_endian_value_1>...><var_uint_name_size_N><name_data_N><int16_little_endian_value_N>
  # Tuple(T1, ..., TN)	0x1F <var_uint_number_of_elements><nested_type_encoding_1>...<nested_type_encoding_N>
  # Tuple(name1 T1, ..., nameN TN)	0x20 <var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>
  # Set	0x21
  # Interval	0x22 <interval_kind> (see interval kind binary encoding)
  # Function	0x24<var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N><return_type_encoding>
  # AggregateFunction(function_name(param_1, ..., param_N), arg_T1, ..., arg_TN)	0x25<var_uint_version><var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
  # Map(K, V)	0x27<key_type_encoding><value_type_encoding>
  # Variant(T1, ..., TN)	0x2A<var_uint_number_of_variants><variant_type_encoding_1>...<variant_type_encoding_N>
  # Dynamic(max_types=N)	0x2B<uint8_max_types>
  # Custom type (Ring, Polygon, etc)	0x2C<var_uint_type_name_size><type_name_data>
  # SimpleAggregateFunction(function_name(param_1, ..., param_N), arg_T1, ..., arg_TN)	0x2E<var_uint_function_name_size><function_name_data><var_uint_number_of_parameters><param_1>...<param_N><var_uint_number_of_arguments><argument_type_encoding_1>...<argument_type_encoding_N> (see aggregate function parameter binary encoding)
  # Nested(name1 T1, ..., nameN TN)	0x2F<var_uint_number_of_elements><var_uint_name_size_1><name_data_1><nested_type_encoding_1>...<var_uint_name_size_N><name_data_N><nested_type_encoding_N>

  unsupported_dynamic_types = %{
    "Enum8" => 0x17,
    "Enum16" => 0x18,
    "Tuple" => 0x1F,
    "TupleWithNames" => 0x20,
    "Set" => 0x21,
    "Interval" => 0x22,
    "Function" => 0x24,
    "AggregateFunction" => 0x25,
    "Map" => 0x27,
    "Variant" => 0x2A,
    "Dynamic" => 0x2B,
    "CustomType" => 0x2C,
    "SimpleAggregateFunction" => 0x2E,
    "Nested" => 0x2F
  }

  for {type, code} <- unsupported_dynamic_types do
    defp decode_dynamic(<<unquote(code), _::bytes>>, _dynamic, _types_rest, _row, _rows, _types) do
      raise ArgumentError, "unsupported dynamic type #{unquote(type)}"
    end
  end

  defp decode_dynamic(<<bin::bytes>>, dynamic, types_rest, row, rows, _types) do
    to_be_continued(rows, bin, [{:dynamic, dynamic} | types_rest], row)
  end

  @compile inline: [decode_dynamic_continue: 6]

  defp decode_dynamic_continue(<<rest::bytes>>, dynamic, types_rest, row, rows, types) do
    continue? =
      case dynamic do
        [:array | _] -> true
        [:nullable | _] -> true
        [:low_cardinality | _] -> true
        _ -> false
      end

    if continue? do
      decode_dynamic(rest, dynamic, types_rest, row, rows, types)
    else
      type = build_dynamic_type(:lists.reverse(dynamic))
      decode_rows([type | types_rest], rest, row, rows, types)
    end
  end

  defp build_dynamic_type([type]), do: type

  defp build_dynamic_type(type) do
    case type do
      [:array | rest] -> {:array, build_dynamic_type(rest)}
      [:nullable | rest] -> {:nullable, build_dynamic_type(rest)}
      [:low_cardinality | rest] -> build_dynamic_type(rest)
    end
  end

  simple_types = %{
    u8: %{pattern: quote(do: <<u>>), value: quote(do: u)},
    u16: %{pattern: quote(do: <<u::16-little>>), value: quote(do: u)},
    u32: %{pattern: quote(do: <<u::32-little>>), value: quote(do: u)},
    u64: %{pattern: quote(do: <<u::64-little>>), value: quote(do: u)},
    u128: %{pattern: quote(do: <<u::128-little>>), value: quote(do: u)},
    u256: %{pattern: quote(do: <<u::256-little>>), value: quote(do: u)},
    i8: %{pattern: quote(do: <<i::signed>>), value: quote(do: i)},
    i16: %{pattern: quote(do: <<i::16-little-signed>>), value: quote(do: i)},
    i32: %{pattern: quote(do: <<i::32-little-signed>>), value: quote(do: i)},
    i64: %{pattern: quote(do: <<i::64-little-signed>>), value: quote(do: i)},
    i128: %{pattern: quote(do: <<i::128-little-signed>>), value: quote(do: i)},
    i256: %{pattern: quote(do: <<i::256-little-signed>>), value: quote(do: i)},
    f32: [
      %{pattern: quote(do: <<f::32-little-float>>), value: quote(do: f)},
      %{pattern: quote(do: <<_nan_or_inf::32>>), value: quote(do: nil)}
    ],
    f64: [
      %{pattern: quote(do: <<f::64-little-float>>), value: quote(do: f)},
      %{pattern: quote(do: <<_nan_or_inf::64>>), value: quote(do: nil)}
    ],
    uuid: %{
      pattern: quote(do: <<u1::64-little, u2::64-little>>),
      value: quote(do: <<u1::64, u2::64>>)
    },
    date: %{
      pattern: quote(do: <<d::16-little>>),
      value: quote(do: Date.add(@epoch_date, d))
    },
    date32: %{
      pattern: quote(do: <<d::32-little-signed>>),
      value: quote(do: Date.add(@epoch_date, d))
    },
    time: %{
      pattern: quote(do: <<s::32-little-signed>>),
      value: quote(do: time_after_midnight(s, 1))
    },
    boolean: [
      %{pattern: quote(do: <<0>>), value: quote(do: false)},
      %{pattern: quote(do: <<1>>), value: quote(do: true)},
      %{pattern: quote(do: <<b>>), value: quote(do: raise("invalid boolean value: #{b}"))}
    ],
    ipv4: %{
      pattern: quote(do: <<b4, b3, b2, b1>>),
      value: quote(do: {b1, b2, b3, b4})
    },
    ipv6: %{
      pattern: quote(do: <<b1::16, b2::16, b3::16, b4::16, b5::16, b6::16, b7::16, b8::16>>),
      value: quote(do: {b1, b2, b3, b4, b5, b6, b7, b8})
    },
    point: %{
      pattern: quote(do: <<x::64-little-float, y::64-little-float>>),
      value: quote(do: {x, y})
    }
  }

  for {type, clauses} <- simple_types do
    fun = :"decode_#{type}_decode_rows"
    @compile inline: [{fun, 5}]

    for %{pattern: pattern, value: value} <- List.wrap(clauses) do
      defp unquote(fun)(<<unquote(pattern), rest::bytes>>, types_rest, row, rows, types) do
        decode_rows(types_rest, rest, [unquote(value) | row], rows, types)
      end
    end

    defp unquote(fun)(<<bin::bytes>>, types_rest, row, rows, _types) do
      to_be_continued(rows, bin, [unquote(type) | types_rest], row)
    end
  end

  defp decode_rows([type | types_rest], <<bin::bytes>>, row, rows, types) do
    case type do
      :u8 ->
        decode_u8_decode_rows(bin, types_rest, row, rows, types)

      :u16 ->
        decode_u16_decode_rows(bin, types_rest, row, rows, types)

      :u32 ->
        decode_u32_decode_rows(bin, types_rest, row, rows, types)

      :u64 ->
        decode_u64_decode_rows(bin, types_rest, row, rows, types)

      :u128 ->
        decode_u128_decode_rows(bin, types_rest, row, rows, types)

      :u256 ->
        decode_u256_decode_rows(bin, types_rest, row, rows, types)

      :i8 ->
        decode_i8_decode_rows(bin, types_rest, row, rows, types)

      :i16 ->
        decode_i16_decode_rows(bin, types_rest, row, rows, types)

      :i32 ->
        decode_i32_decode_rows(bin, types_rest, row, rows, types)

      :i64 ->
        decode_i64_decode_rows(bin, types_rest, row, rows, types)

      :i128 ->
        decode_i128_decode_rows(bin, types_rest, row, rows, types)

      :i256 ->
        decode_i256_decode_rows(bin, types_rest, row, rows, types)

      :f32 ->
        decode_f32_decode_rows(bin, types_rest, row, rows, types)

      :f64 ->
        decode_f64_decode_rows(bin, types_rest, row, rows, types)

      :string ->
        decode_string_decode_rows(bin, types_rest, row, rows, types)

      :binary ->
        decode_binary_decode_rows(bin, types_rest, row, rows, types)

      :json ->
        # assuming it arrives as text and not "native" binary JSON
        # i.e. assumes `settings: [output_format_binary_write_json_as_string: 1]`
        # TODO
        decode_string_json_decode_rows(bin, types_rest, row, rows, types)

      :dynamic ->
        decode_dynamic(bin, _dynamic = [], types_rest, row, rows, types)

      {:dynamic, dynamic} ->
        decode_dynamic(bin, dynamic, types_rest, row, rows, types)

      # TODO utf8?
      {:fixed_string, size} ->
        case bin do
          <<s::size(^size)-bytes, rest::bytes>> ->
            decode_rows(types_rest, rest, [s | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      :boolean ->
        decode_boolean_decode_rows(bin, types_rest, row, rows, types)

      :uuid ->
        decode_uuid_decode_rows(bin, types_rest, row, rows, types)

      :date ->
        decode_date_decode_rows(bin, types_rest, row, rows, types)

      :date32 ->
        decode_date32_decode_rows(bin, types_rest, row, rows, types)

      :time ->
        decode_time_decode_rows(bin, types_rest, row, rows, types)

      {:time64, time_unit} ->
        case bin do
          <<ticks::64-little-signed, bin::bytes>> ->
            time = time_after_midnight(ticks, time_unit)
            decode_rows(types_rest, bin, [time | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:datetime, timezone} ->
        case bin do
          <<s::32-little, bin::bytes>> ->
            dt =
              case timezone do
                nil -> NaiveDateTime.add(@epoch_naive_datetime, s)
                "UTC" -> DateTime.from_unix!(s)
                _ -> s |> DateTime.from_unix!() |> DateTime.shift_zone!(timezone)
              end

            decode_rows(types_rest, bin, [dt | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:decimal, size, scale} ->
        case bin do
          <<val::size(^size)-little-signed, bin::bytes>> ->
            sign = if val < 0, do: -1, else: 1
            d = Decimal.new(sign, abs(val), -scale)
            decode_rows(types_rest, bin, [d | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:nullable, inner_type} ->
        case bin do
          <<b, bin::bytes>> ->
            case b do
              0 -> decode_rows([inner_type | types_rest], bin, row, rows, types)
              1 -> decode_rows(types_rest, bin, [nil | row], rows, types)
            end

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      :nothing ->
        decode_rows(types_rest, bin, [nil | row], rows, types)

      {:array, inner_type} ->
        decode_array_decode_rows(bin, inner_type, types_rest, row, rows, types)

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

      {:variant, variant_types} ->
        case bin do
          <<255, bin::bytes>> ->
            # 255 is the variant type index for "nothing"
            decode_rows(types_rest, bin, [nil | row], rows, types)

          # TODO varint?
          <<variant_type_index::8, bin::bytes>> ->
            variant_type = Enum.at(variant_types, variant_type_index)
            decode_rows([variant_type | types_rest], bin, row, rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:datetime64, time_unit, timezone} ->
        case bin do
          <<s::64-little-signed, bin::bytes>> ->
            dt =
              case timezone do
                nil ->
                  NaiveDateTime.add(@epoch_naive_datetime, s, time_unit)
                  |> truncate(time_unit)

                "UTC" ->
                  DateTime.from_unix!(s, time_unit)

                _ ->
                  s
                  |> DateTime.from_unix!(time_unit)
                  |> DateTime.shift_zone!(timezone)
              end

            decode_rows(types_rest, bin, [dt | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:enum8, mapping} ->
        case bin do
          <<v::signed, bin::bytes>> ->
            decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      {:enum16, mapping} ->
        case bin do
          <<v::16-little-signed, bin::bytes>> ->
            decode_rows(types_rest, bin, [Map.fetch!(mapping, v) | row], rows, types)

          _ ->
            to_be_continued(rows, bin, [type | types_rest], row)
        end

      :ipv4 ->
        decode_ipv4_decode_rows(bin, types_rest, row, rows, types)

      :ipv6 ->
        decode_ipv6_decode_rows(bin, types_rest, row, rows, types)

      :point ->
        decode_point_decode_rows(bin, types_rest, row, rows, types)
    end
  end

  defp decode_rows([], <<>> = empty, row, rows, _types) do
    rows = :lists.reverse([:lists.reverse(row) | rows])
    {rows, empty, _no_state = nil}
  end

  defp decode_rows([], <<bin::bytes>>, row, rows, types) do
    row = :lists.reverse(row)
    decode_rows(types, bin, [], [row | rows], types)
  end

  defp decode_rows([_ | _] = types_rest, <<>> = empty, row, rows, _types) do
    to_be_continued(rows, empty, types_rest, row)
  end

  @compile inline: [to_be_continued: 4]
  defp to_be_continued(rows, bin, types_rest, row) do
    {:lists.reverse(rows), bin, {:cont, types_rest, row}}
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

  @compile inline: [time_unit: 1, time_prec: 1]
  for precision <- 0..9 do
    time_unit = round(:math.pow(10, precision))

    defp time_unit(unquote(precision)), do: unquote(time_unit)
    defp time_prec(unquote(time_unit)), do: unquote(precision)
  end

  defp truncate(%{microsecond: {micros, _prec}} = date, time_unit),
    do: %{date | microsecond: {micros, time_prec(time_unit)}}

  @compile inline: [time_after_midnight: 2]
  defp time_after_midnight(ticks, time_unit) do
    if ticks >= 0 and ticks < 86400 * time_unit do
      ticks |> DateTime.from_unix!(time_unit) |> DateTime.to_time()
    else
      # since ClickHouse supports Time64 values of [-999:59:59.999999999, 999:59:59.999999999]
      # and Elixir's Time supports values of [00:00:00.000000, 23:59:59.999999]
      # we raise an error when ClickHouse's Time64 value is out of Elixir's Time range
      raise ArgumentError,
            "ClickHouse Time value #{:erlang.float_to_binary(ticks / time_unit, [:short])} (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)"

      # TODO: we could potentially decode ClickHouse's Time/Time64 values as Elixir's Duration when it's out of Elixir's Time range
    end
  end
end
