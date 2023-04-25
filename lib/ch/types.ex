defmodule Ch.Types do
  @moduledoc """
  Helpers to turn ClickHouse types into Elixir terms for easier processing.
  """

  types =
    [
      {_encoded = "String", _decoded = :string, _args = []},
      {"Bool", :boolean, []},
      for size <- [8, 16, 32, 64, 128, 256] do
        [
          {"UInt#{size}", :"u#{size}", []},
          {"Int#{size}", :"i#{size}", []}
        ]
      end,
      for size <- [32, 64] do
        {"Float#{size}", :"f#{size}", []}
      end,
      {"Array", :array, [:type]},
      {"Tuple", :tuple, [:type]},
      {"Map", :map, [:type]},
      {"FixedString", :fixed_string, [:int]},
      {"Nullable", :nullable, [:type]},
      {"DateTime64", :datetime64, [:int, :string]},
      {"DateTime", :datetime, [:string]},
      # {"DateTime", :datetime, []},
      {"Date32", :date32, []},
      {"Date", :date, []},
      {"LowCardinality", :low_cardinality, [:type]},
      for size <- [32, 64, 128, 256] do
        {"Decimal#{size}", :"decimal#{size}", [:int]}
      end,
      {"Decimal", :decimal, [:int, :int]},
      {"SimpleAggregateFunction", :simple_aggregate_function, [:identifier, :type]},
      {"Enum8", :enum8, [:string, :eq, :int]},
      {"Enum16", :enum16, [:string, :eq, :int]},
      {"UUID", :uuid, []},
      {"IPv4", :ipv4, []},
      {"IPv6", :ipv6, []},
      {"Point", :point, []},
      {"Ring", :ring, []},
      {"Polygon", :polygon, []},
      {"MultiPolygon", :multipolygon, []},
      {"Nothing", :nothing, []}
    ]
    |> List.flatten()

  for {encoded, name, []} <- types do
    @doc """
    Helper for `#{encoded}` ClickHouse type:

        iex> #{name}()
        :#{name}

        iex> encode(#{name}())
        "#{encoded}"

        iex> decode("#{encoded}")
        #{name}()

    """
    def unquote(name)(), do: unquote(name)
  end

  @doc """
  Helper for `DateTime` ClickHouse type:

      iex> datetime()
      :datetime

      iex> to_string(encode(datetime()))
      "DateTime"

      iex> decode("DateTime")
      datetime()

  """
  def datetime, do: :datetime

  @doc """
  Helper for `DateTime(timezone)` ClickHouse type:

      iex> datetime("Europe/Vienna")
      {:datetime, "Europe/Vienna"}

      iex> to_string(encode(datetime("UTC")))
      "DateTime('UTC')"

      iex> decode("DateTime('UTC')")
      datetime("UTC")

  """
  def datetime(timezone) when is_binary(timezone), do: {:datetime, timezone}

  @doc """
  Helper for `DateTime64(precision)` ClickHouse type:

      iex> datetime64(3)
      {:datetime64, 3}

      iex> to_string(encode(datetime64(3)))
      "DateTime64(3)"

      iex> decode("DateTime64(3)")
      datetime64(3)

  """
  def datetime64(precision) when is_integer(precision), do: {:datetime64, precision}

  @doc """
  Helper for `DateTime64(precision, timezone)` ClickHouse type:

      iex> datetime64(3, "UTC")
      {:datetime64, 3, "UTC"}

      iex> to_string(encode(datetime64(3, "UTC")))
      "DateTime64(3, 'UTC')"

      iex> decode("DateTime64(3, 'UTC')")
      datetime64(3, "UTC")

  """
  def datetime64(precision, timezone) when is_integer(precision) and is_binary(timezone) do
    {:datetime64, precision, timezone}
  end

  @doc """
  Helper for `FixedString(n)` ClickHouse type:

      iex> fixed_string(3)
      {:fixed_string, 3}

      iex> to_string(encode(fixed_string(16)))
      "FixedString(16)"

      iex> decode("FixedString(16)")
      fixed_string(16)

  """
  def fixed_string(n) when is_integer(n), do: {:fixed_string, n}

  @doc """
  Helper for `Decimal(P, S)` ClickHouse type:

      iex> decimal(18, 4)
      {:decimal, 18, 4}

      iex> to_string(encode(decimal(18, 4)))
      "Decimal(18, 4)"

      iex> decode("Decimal(18, 4)")
      decimal(18, 4)

  """
  def decimal(precision, scale) when is_integer(precision) and is_integer(scale) do
    {:decimal, precision, scale}
  end

  for size <- [32, 64, 128, 256] do
    name = :"decimal#{size}"

    # `select toTypeName(cast(1 as Decimal32(2)))` etc.
    precision =
      case size do
        32 -> 9
        64 -> 18
        128 -> 38
        256 -> 76
      end

    @doc """
    Helper for `Decimal#{size}(S)` ClickHouse type:

        iex> #{name}(4)
        {:#{name}, 4}

        iex> to_string(encode(#{name}(4)))
        "Decimal(#{precision}, 4)"

        iex> decode("Decimal#{size}(4)")
        {:#{name}, 4}

    """
    def unquote(name)(scale) when is_integer(scale), do: {unquote(name), scale}
  end

  defguardp is_type(type) when is_atom(type) or is_tuple(type)

  @doc """
  Helper for `Array(T)` ClickHouse type:

      iex> array(u64())
      {:array, :u64}

      iex> to_string(encode(array(u64())))
      "Array(UInt64)"

      iex> decode("Array(UInt64)")
      array(u64())

  """
  def array(type) when is_type(type), do: {:array, type}

  @doc """
  Helper for `Tuple(T1, T2, ...)` ClickHouse type:

      iex> tuple([u64(), array(string())])
      {:tuple, [:u64, {:array, :string}]}

      iex> to_string(encode(tuple([u64(), array(string())])))
      "Tuple(UInt64, Array(String))"

      iex> decode("Tuple(UInt64, Array(String))")
      tuple([u64(), array(string())])

  """
  def tuple(types) when is_list(types), do: {:tuple, types}

  @doc """
  Helper for `Map(K, V)` ClickHouse type:

      iex> map(string(), array(string()))
      {:map, :string, {:array, :string}}

      iex> to_string(encode(map(string(), array(string()))))
      "Map(String, Array(String))"

      iex> decode("Map(String, Array(String))")
      map(string(), array(string()))

  """
  def map(key_type, value_type) when is_type(key_type) and is_type(value_type) do
    {:map, key_type, value_type}
  end

  @doc """
  Helper for `Nullable(T)` ClickHouse type:

      iex> nullable(array(boolean()))
      {:nullable, {:array, :boolean}}

      iex> to_string(encode(nullable(array(boolean()))))
      "Nullable(Array(Bool))"

      iex> decode("Nullable(Array(Bool))")
      nullable(array(boolean()))

  """
  def nullable(type) when is_type(type), do: {:nullable, type}

  @doc """
  Helper for `LowCardinality(T)` ClickHouse type:

      iex> low_cardinality(string())
      {:low_cardinality, :string}

      iex> to_string(encode(low_cardinality(string())))
      "LowCardinality(String)"

      iex> decode("LowCardinality(String)")
      low_cardinality(string())

  """
  def low_cardinality(type) when is_type(type), do: {:low_cardinality, type}

  @doc """
  Helper for `SimpleAggregateFunction(name, type)` ClickHouse type:

      iex> simple_aggregate_function("any", u8())
      {:simple_aggregate_function, "any", :u8}

      iex> to_string(encode(simple_aggregate_function("any", u8())))
      "SimpleAggregateFunction(any, UInt8)"

      iex> decode("SimpleAggregateFunction(any, UInt8)")
      simple_aggregate_function("any", u8())

  """
  def simple_aggregate_function(name, type) when is_binary(name) and is_type(type) do
    {:simple_aggregate_function, name, type}
  end

  for size <- [8, 16] do
    name = :"enum#{size}"

    @doc """
    Helper for `Enum#{size}` ClickHouse type:

        iex> #{name}([{"hello", 1}, {"world", 2}])
        {:#{name}, [{"hello", 1}, {"world", 2}]}

        iex> to_string(encode(#{name}([{"hello", 1}, {"world", 2}])))
        "Enum#{size}('hello' = 1, 'world' = 2)"

        iex> decode("Enum#{size}('hello' = 1, 'world' = 2)")
        #{name}([{"hello", 1}, {"world", 2}])

    """
    def unquote(name)(mapping) when is_list(mapping), do: {unquote(name), mapping}
  end

  @doc """
  Decodes a ClickHouse type into an intermediary Elixir term.

      iex> decode("String")
      :string

      iex> decode("Array(String)")
      {:array, :string}

      iex> decode("Enum8('hello' = 1, 'world' = 2)")
      {:enum8, [{"hello", 1}, {"world", 2}]}

      iex> decode("Nullable(Decimal(18, 4))")
      {:nullable, {:decimal, 18, 4}}

  """
  def decode(type)

  for {encoded, decoded, []} <- types do
    def decode(unquote(encoded)), do: unquote(decoded)
  end

  def decode("DateTime"), do: :datetime

  def decode(type) do
    try do
      decode([:type], type, [])
    rescue
      e ->
        message = "failed to decode #{inspect(type)} as ClickHouse type (#{Exception.message(e)})"
        reraise(ArgumentError, message, __STACKTRACE__)
    end
  end

  defguardp is_whitespace(char) when char == ?\s or char == ?\t

  defp decode(stack, <<whitespace, rest::bytes>>, acc) when is_whitespace(whitespace) do
    decode(stack, rest, acc)
  end

  for {encoded, decoded, [_ | _] = args} <- types do
    defp decode([:type | stack], unquote(encoded) <> rest, acc) do
      decode(
        [:open | unquote(args)] ++ [:close, {unquote(decoded), unquote(args)}, acc | stack],
        rest,
        []
      )
    end
  end

  for {encoded, decoded, []} <- types do
    defp decode([:type | stack], unquote(encoded) <> rest, acc) do
      decode(stack, rest, [unquote(decoded) | acc])
    end
  end

  defp decode([:open | stack], <<rest::bytes>>, acc) do
    case rest do
      <<?(, rest::bytes>> ->
        decode(stack, rest, acc)

      _ ->
        # handles DateTime and Type()
        [{type, _args}, prev_acc | stack] = close(stack)
        decode(stack, rest, [type | prev_acc])
    end
  end

  defp decode(stack, <<?), rest::bytes>>, acc) do
    [{type, _args}, prev_acc | stack] = close(stack)
    decode(stack, rest, [build_type(type, acc) | prev_acc])
  end

  defp decode([:close, {_type, args} | _] = stack, <<?,, rest::bytes>>, acc) do
    decode(args ++ stack, rest, acc)
  end

  defp decode(stack, <<?,, rest::bytes>>, acc) do
    decode(stack, rest, acc)
  end

  defp decode([:string | stack], <<?', rest::bytes>>, acc) do
    decode_string(rest, 0, rest, stack, acc)
  end

  defp decode([:int | stack], <<rest::bytes>>, acc) do
    decode_int(rest, stack, acc)
  end

  defp decode([:identifier | stack], <<rest::bytes>>, acc) do
    decode_identifier(rest, 0, rest, stack, acc)
  end

  defp decode([:eq | stack], <<?=, rest::bytes>>, acc) do
    decode(stack, rest, acc)
  end

  defp decode([], <<>>, [type]), do: type

  defp close([:close | stack]), do: stack
  defp close([_ | stack]), do: close(stack)

  defp build_type(:array = a, [t]), do: {a, t}
  defp build_type(:tuple = t, ts), do: {t, :lists.reverse(ts)}
  defp build_type(:fixed_string = fs, [n]), do: {fs, n}
  defp build_type(:datetime = d, [tz]), do: {d, tz}
  defp build_type(:datetime64 = d, [precision]), do: {d, precision}
  defp build_type(:datetime64 = d, [tz, p]), do: {d, p, tz}
  defp build_type(:map = m, [v, k]), do: {m, k, v}
  defp build_type(:nullable = n, [t]), do: {n, t}
  defp build_type(:low_cardinality = l, [t]), do: {l, t}
  defp build_type(:enum8 = e, mapping), do: {e, build_enum_mapping(mapping)}
  defp build_type(:enum16 = e, mapping), do: {e, build_enum_mapping(mapping)}
  defp build_type(:simple_aggregate_function = saf, [t, f]), do: {saf, f, t}
  defp build_type(:decimal32 = d, [s]), do: {d, s}
  defp build_type(:decimal64 = d, [s]), do: {d, s}
  defp build_type(:decimal128 = d, [s]), do: {d, s}
  defp build_type(:decimal256 = d, [s]), do: {d, s}
  defp build_type(:decimal = d, [s, p]), do: {d, p, s}

  defp build_enum_mapping(mapping) do
    mapping |> :lists.reverse() |> Enum.chunk_every(2) |> Enum.map(fn [k, v] -> {k, v} end)
  end

  # TODO '', \'

  defp decode_string(<<?', rest::bytes>>, len, original, stack, acc) do
    part = :binary.part(original, 0, len)
    decode(stack, rest, [:binary.copy(part) | acc])
  end

  defp decode_string(<<u::utf8, rest::bytes>>, len, original, stack, acc) do
    decode_string(rest, len + utf8_size(u), original, stack, acc)
  end

  @compile inline: [utf8_size: 1]
  defp utf8_size(codepoint) when codepoint <= 0x7F, do: 1
  defp utf8_size(codepoint) when codepoint <= 0x7FF, do: 2
  defp utf8_size(codepoint) when codepoint <= 0xFFFF, do: 3
  defp utf8_size(codepoint) when codepoint <= 0x10FFFF, do: 4

  defguardp is_alpha(a) when (a >= ?a and a <= ?z) or (a >= ?A and a <= ?Z)

  defp decode_identifier(<<a, rest::bytes>>, len, original, stack, acc) when is_alpha(a) do
    decode_identifier(rest, len + 1, original, stack, acc)
  end

  defp decode_identifier(<<rest::bytes>>, len, original, stack, acc) do
    part = :binary.part(original, 0, len)
    decode(stack, rest, [:binary.copy(part) | acc])
  end

  defguardp is_numeric(char) when char >= ?0 and char <= ?9

  defp decode_int(<<?-, i, rest::bytes>>, stack, outer_acc) when is_numeric(i) do
    decode_int_cont(rest, -(i - ?0), stack, outer_acc)
  end

  defp decode_int(<<i, rest::bytes>>, stack, outer_acc) when is_numeric(i) do
    decode_int_cont(rest, i - ?0, stack, outer_acc)
  end

  defp decode_int_cont(<<i, rest::bytes>>, acc, stack, outer_acc) when is_numeric(i) do
    decode_int_cont(rest, acc * 10 + i - ?0, stack, outer_acc)
  end

  defp decode_int_cont(<<rest::bytes>>, int, stack, acc) do
    decode(stack, rest, [int | acc])
  end

  @doc """
  Encodes a type from Elixir atom / tuple to proper ClickHouse name.

      iex> encode(:string)
      "String"

      iex> IO.iodata_to_binary(encode({:nullable, :i8}))
      "Nullable(Int8)"

  """
  def encode(type)

  for {encoded, decoded, []} <- types do
    def encode(unquote(decoded)), do: unquote(encoded)
  end

  def encode(:datetime), do: "DateTime"
  def encode({:nullable, type}), do: ["Nullable(", encode(type), ?)]
  def encode({:fixed_string, n}), do: ["FixedString(", String.Chars.Integer.to_string(n), ?)]
  def encode({:array, type}), do: ["Array(", encode(type), ?)]
  def encode({:tuple, types}), do: ["Tuple(", encode_intersperse(types, ", "), ?)]

  def encode({:map, key_type, value_type}) do
    ["Map(", encode(key_type), ", ", encode(value_type), ?)]
  end

  def encode({:low_cardinality, type}), do: ["LowCardinality(", encode(type), ?)]

  for size <- [32, 64, 128, 256] do
    # `select toTypeName(cast(1 as Decimal32(2)))` etc.
    precision =
      case size do
        32 -> 9
        64 -> 18
        128 -> 38
        256 -> 76
      end

    def encode({unquote(:"decimal#{size}"), scale}) do
      encode({:decimal, unquote(precision), scale})
    end
  end

  def encode({:decimal, precision, scale}) do
    [
      "Decimal(",
      String.Chars.Integer.to_string(precision),
      ", ",
      String.Chars.Integer.to_string(scale),
      ?)
    ]
  end

  def encode({:datetime, timezone}) when is_binary(timezone) do
    ["DateTime('", timezone, "')"]
  end

  def encode({:datetime64, precision}) do
    ["DateTime64(", String.Chars.Integer.to_string(precision), ?)]
  end

  def encode({:datetime64, precision, timezone}) when is_binary(timezone) do
    ["DateTime64(", String.Chars.Integer.to_string(precision), ", '", timezone, "')"]
  end

  def encode({:enum8, mapping}) do
    ["Enum8('", encode_mapping(mapping), ?)]
  end

  def encode({:enum16, mapping}) do
    ["Enum16('", encode_mapping(mapping), ?)]
  end

  def encode({:simple_aggregate_function, name, type}) when is_binary(name) do
    ["SimpleAggregateFunction(", name, ", ", encode(type), ?)]
  end

  defp encode_intersperse([last_type], _separator) do
    [encode(last_type)]
  end

  defp encode_intersperse([type | types], separator) do
    [encode(type), separator | encode_intersperse(types, separator)]
  end

  defp encode_intersperse([] = empty, _separator), do: empty

  defp encode_mapping([{k, v}]) when is_binary(k) do
    [k, "' = ", String.Chars.Integer.to_string(v)]
  end

  defp encode_mapping([{k, v} | mapping]) when is_binary(k) do
    [k, "' = ", String.Chars.Integer.to_string(v), ", '" | encode_mapping(mapping)]
  end

  defp encode_mapping([] = empty), do: empty
end
