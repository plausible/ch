defmodule Ch.TypesTest do
  use ExUnit.Case, async: true

  describe "decode_type/1" do
    test "scalar types" do
      assert decode_type("String") == :string
      assert decode_type("Int8") == :i8
      assert decode_type("UInt8") == :u8
    end

    test "array" do
      assert decode_type("Array(Int8)") == {:array, :i8}
      assert decode_type("Array(String)") == {:array, :string}
      assert decode_type("Array(Array(String))") == {:array, {:array, :string}}
      assert decode_type("Array(Tuple(UInt8, String))") == {:array, {:tuple, [:u8, :string]}}
    end

    test "tuple" do
      assert decode_type("Tuple(UInt8, UInt8)") == {:tuple, [:u8, :u8]}

      assert decode_type("Tuple(Array(String), Tuple(String, UInt64))") ==
               {:typle, [{:array, :string}, {:tuple, [:string, :u64]}]}
    end

    test "datetime" do
      assert decode_type("DateTime") == :datetime
      assert decode_type("DateTime('UTC')") = {:datetime, "Etc/UTC"}
      assert decode_type("DateTime('Asia/Tokyo')") == {:datetime, "Asia/Tokyo"}
    end

    test "datetime64" do
      assert decode_type("DateTime64(3)") == {:datetime64, 3}
      assert decode_type("DateTime64(3, 'UTC')") == {:datetime64, 3, "Etc/UTC"}
      assert decode_type("DateTime64(3, 'Asia/Tokyo')") == {:datetime64, 3, "Asia/Tokyo"}
    end

    test "fixed_string" do
      assert decode_type("FixedString(3)") == {:fixed_string, 3}
      assert decode_type("FixedString(16)") == {:fixed_string, 16}
    end

    test "map" do
      assert decode_type("Map(String, UInt64)") == {:map, :string, :u64}
      assert decode_type("Map(String, Array(String))") == {:map, :string, {:array, :string}}
    end

    test "simple aggregate function" do
      assert decode_type("SimpleAggregateFunction(any, UInt64)") == :u64

      assert decode_type("SimpleAggregateFunction(any, Map(String, UInt64))") ==
               {:map, :string, :u64}
    end

    test "nullable" do
      assert decode_type("Nullable(String)") == {:nullable, :string}
    end
  end

  # or :erlang.reraise
  # TODO try do rescue _ -> raise ArgumentError, "failed to decode #{inspect(type)} as ClickHouse type" end
  def decode_type(type), do: decode_type(type, nil, [], [])

  types =
    [
      {"String", :string},
      {"Bool", :boolean},
      for size <- [8, 16, 32, 64, 128, 256] do
        [
          {"UInt#{size}", :"u#{size}"},
          {"Int#{size}", :"i#{size}"}
        ]
      end,
      for size <- [32, 64] do
        {"Float#{size}", :"f#{size}"}
      end,
      {"UUID", :uuid},
      {"IPv4", :ipv4},
      {"IPv6", :ipv6},
      {"Point", :point},
      {"Ring", :ring},
      {"Polygon", :polygon},
      {"MultiPolygon", :multipolygon}
    ]
    |> List.flatten()

  for {encoded, decoded} <- types do
    def decode_type(unquote(encoded), nil, [], []), do: unquote(decoded)

    def decode_type(unquote(encoded), current, acc, stack) do
      decode_type(rest, current, [unquote(decoded) | acc], stack)
    end
  end

  def decode_type("Array(" <> rest, acc, stack) do
    decode_type(rest, [], [{:array, acc} | stack])
  end

  def decode_type("Tuple(" <> rest, acc, stack) do
    decode_type(rest, [], [{:tuple, acc} | stack])
  end

  def decode_type(<<?), rest::bytes>>, acc, stack) do
    case stack do
      [] ->
        decode_type(rest, acc, stack)

      [frame | stack] ->
        case frame do
          {:array, original_acc} ->
            [type] = acc
            decode_type(rest, [{:array, type} | original_acc], stack)

          {:tuple, original_acc} ->
            decode_type(rest, [{:tuple, :lists.reverse(acc)} | original_acc], stack)
        end
    end
  end

  def decode_type(<<?,, rest::bytes>>, acc, [{allowed, _} | stack]) do
    decode_type(rest, acc, stack)
  end

  def decode_type(<<?\s, rest::bytes>>, acc, stack) do
    decode_type(rest, acc, stack)
  end

  def decode_type("", _current, [type], _stack = []), do: type

  def decode_param_int(<<i, rest::bytes>>, acc, terminators, stack) when i >= ?0 and i <= ?9 do
    decode_param_int(rest, acc * 10 + i - ?0, terminators)
  end

  def decode_param_int(<<t, rest::bytes>>, acc, terminators, stack) do
    case t in terminators do
      true -> decode_type_param(stack, acc, rest)
    end
  end
end

# TODO need acc to be list?
# TODO can [:array, _prev_acc = []] be used instead of array_over, tuple_over, etc.?
# TODO DateTime64 ( 3 , 'Asia/Taipei' )
# stack approach

decode([:type], "String", _acc = [])
decode([], "", _acc = [:string])

decode([:type], "Array(String)", _acc = [])
decode([:type, {:array_over, _acc = []}], "String)", _acc = [])
decode([{:array_over, _acc = []}], ")", _acc = [:string])
decode([], "", _acc = [{:array, [:string]}])

decode([:type], "Array(Array(String))", _acc = [])
decode([:type, {:array_over, _acc = []}], "Array(String))", _acc = [])
decode([:type, {:array_over, _acc = []}, {:array_over, _acc = []}], "String))", _acc = [])
decode([{:array_over, _acc = []}, {:array_over, _acc = []}], "))", _acc = [:string])
# array_over + ) -> end
decode([{:array_over, _acc = []}], ")", _acc = [{:array, :string}])
decode([], "", _acc = [{:array, {:array, :string}}])

decode([:type], "Tuple(String, UInt64)", [])
decode([:type, {:tuple_over, []}], "String, UInt64)", [])
# when processing :tuple_over, ',' are eaten
decode([{:tuple_over, []}], ", UInt64)", [:string])
# when processing :type, whitespace is ignored
decode([:type, {:tuple_over, []}], " UInt64)", [:string])
decode([:type, {:tuple_over, []}], "UInt64)", [:string])
# tuple_over + ) -> real end
decode([{:tuple_over, []}], ")", [:u64, :string])
decode([], "", [{:tuple, [:string, :u64]}])

decode([:type], "Array(Tuple(String, UInt64))", [])
decode([:type, {:array_over, []}], "Tuple(String, UInt64))", [])
decode([:type, {:tuple_over, []}, {:array_over, []}], "String, UInt64))", [])
decode([{:tuple_over, []}, {:array_over, []}], ", UInt64))", [:string])
decode([:type, {:tuple_over, []}, {:array_over, []}], " UInt64))", [:string])
decode([:type, {:tuple_over, []}, {:array_over, []}], "UInt64))", [:string])
decode([{:tuple_over, []}, {:array_over, []}], "))", [:u64, :string])
decode([{:array_over, []}], ")", [{:tuple, [:string, :u64]}])
decode([], "", [{:array, {:tuple, [:string, :u64]}}])

decode([:type], "Tuple(String, Array(String), UInt64)", [])
decode([:type, :tuple, []], "String, Array(String), UInt64)", [])
decode([:tuple, []], ", Array(String), UInt64)", [:string])
decode([:type, :tuple, []], " Array(String), UInt64)", [:string])
decode([:type, :tuple, []], "Array(String), UInt64)", [:string])
decode([:type, :array, [:string], :tuple, []], "String), UInt64)", [])
decode([:array, [:string], :tuple, []], "), UInt64)", [:string])
decode([:tuple, []], ", UInt64)", [{:array, :string}, :string])
decode([:type, :tuple, []], " UInt64)", [{:array, :string}, :string])
decode([:type, :tuple, []], "UInt64)", [{:array, :string}, :string])
decode([:tuple, []], ")", [:u64, {:array, :string}, :string])
decode([], "", [{:tuple, [:string, {:array, :string}, :u64]}])

# TODO '' or \' -> escape
decode([:type], "DateTime('UTC')", [])
decode_str("UTC')", 0, [:datetime, []], [])
decode_str("TC')", 1, [:datetime, []], [])
decode_str("C')", 2, [:datetime, []], [])
decode_str("')", 3, [:datetime, []], [])
decode([:datetime, []], ")", ["UTC"])
decode([], "", [{:datetime, "UTC"}])

decode([:type], "FixedString(16)", [])
decode_int("16)", 0, [:fixed_string, []], [])
decode_int("6)", 1, [:fixed_string, []], [])
decode_int(")", 16, [:fixed_string, []], [])
decode([:fixed_string, []], ")", [])
decode([], "", [{:fixed_string, 16}])

decode([:type], "DateTime64(3, 'UTC')", [])
decode([:str, :datetime64, []], " 'UTC')", [3])
decode([:str, :datetime64, []], "'UTC')", [3])
decode_str("UTC')", 0, [:datetime64, []], [3])
decode_str("TC')", 1, [:datetime64, []], [3])
decode_str("C')", 2, [:datetime64, []], [3])
decode_str("')", 3, [:datetime64, []], [3])
decode([:datetime64, []], ")", ["UTC", 3])
decode([], "", [{:datetime, 3, "UTC"}])

decode([:type], "SimpleAggregateFunction(any, Map(String, UInt64))", [])
decode([:fun, []], "any, Map(String, UInt64))", [])
decode_identifier("any, Map(String, UInt64))", 0, [:fun, []], [])
decode_identifier("ny, Map(String, UInt64))", 1, [:fun, []], [])
decode_identifier("y, Map(String, UInt64))", 2, [:fun, []], [])
decode_identifier(", Map(String, UInt64))", 3, [:fun, []], [])
decode([:fun, []], ", Map(String, UInt64))", ["any"])
decode([:type, :fun, []], " Map(String, UInt64))", ["any"])
decode([:type, :fun, []], "Map(String, UInt64))", ["any"])
decode([:type, :map, ["any"], :fun, []], "String, UInt64))", [])
decode([:map, ["any"], :fun, []], ", UInt64))", [:string])
decode([:type, :map, ["any"], :fun, []], " UInt64))", [:string])
decode([:type, :map, ["any"], :fun, []], "UInt64))", [:string])
decode([:map, ["any"], :fun, []], "))", [:u64, :string])
decode([:fun, []], ")", [{:map, :string, :u64}, "any"])
decode([], "", [{:map, :string, :u64}])

decode([:type], "Enum('hello' = 1, 'world' = 2)", [])
decode([:str, :int, :enum8, []], "'hello' = 1, 'world' = 2)", [])
decode_str("hello' = 1, 'world' = 2)", 0, [:int, :enum8, []], [])
decode_str("' = 1, 'world' = 2)", 5, [:int, :enum8, []], [])
decode([?=, :int, :enum8, []], " = 1, 'world' = 2)", ["hello"])
decode([?=, :int, :enum8, []], "= 1, 'world' = 2)", ["hello"])
decode([:int, :enum8, []], " 1, 'world' = 2)", ["hello"])
decode([:int, :enum8, []], "1, 'world' = 2)", ["hello"])
decode_int("1, 'world' = 2)", 0, [:enum8, []], ["hello"])
decode_int(", 'world' = 2)", 1, [:enum8, []], ["hello"])
decode([:enum8, []], ", 'world' = 2)", [1, "hello"])
decode([:str, ?=, :int, :enum8, []], " 'world' = 2)", [{"hello", 1}])
decode([:str, ?=, :int, :enum8, []], "'world' = 2)", [{"hello", 1}])
decode_str("world' = 2)", 0, [?=, :int, :enum8, []], [{"hello", 1}])
decode_str("' = 2)", 5, [?=, :int, :enum8, []], [{"hello", 1}])
decode([?=, :int, :enum8, []], " = 2)", [{"hello", 1}])
decode([:int, :enum8, []], "2)", ["world", {"hello", 1}])
# etc.

# non-stack approach

decode_type("Array(String)", nil, _inner_acc = [], _outer_acc = [])
decode_type("String)", :array, [], [])
decode_type(")", :array, [:string], [])
decode_type("", nil, [], [{:array, :string}])

decode_type("Array(Array(String))", nil, [], [])
decode_type("Array(String))", :array, [], [])
decode_type("String))", :array, [], [:array])
decode_type("))", :array, [:string], [:array])
decode_type(")", :array, [{:array, :string}], [])
decode_type("", nil, [], [{:array, {:array, :string}}])

decode_type("Tuple(String, UInt64)", nil, [], [])
decode_type("String, UInt64)", :tuple, [], [])
decode_type(", UInt64)", :tuple, [:string], [])
decode_type(" UInt64)", :tuple, [:string], [])
decode_type("UInt64)", :tuple, [:string], [])
decode_type(")", :tuple, [:u64, :string], [])
decode_type("", nil, [], [{:tuple, [:string, :u64]}])

decode_type("Array(Tuple(String, UInt64))", nil, [], [])
decode_type("Tuple(String, UInt64))", :array, [], [])
decode_type("String, UInt64))", :tuple, [], [:array])
decode_type(", UInt64))", :tuple, [:string], [:array])
decode_type(" UInt64))", :tuple, [:string], [:array])
decode_type("UInt64))", :tuple, [:string], [:array])
decode_type("))", :tuple, [:u64, :string], [:array])
decode_type(")", :array, [{:tuple, [:string, :u64]}], [])
decode_type("", nil, [], [{:array, {:tuple, [:string, :u64]}}])

decode_type("DateTime('UTC')", nil, [], [])
decode_param_str("UTC')", 0, {:datetime, [], []})
decode_param_str("TC')", 1, {:datetime, [], []})
decode_param_str("C')", 2, {:datetime, [], []})
decode_param_str("')", 3, {:datetime, [], []})
decode_type(")", :datetime, ["UTC"], [])
decode_type("", nil, [], [{:datetime, "UTC"}])

decode_type("FixedString(30)", nil, [], [])
decode_param_int("30)", 0, {:fixed_string, [], []})
decode_param_int("0)", 3, {:fixed_string, [], []})
decode_param_int(")", 30, {:fixed_string, [], []})
decode_type(")", :fixed_string, [30], [])
decode_type("", nil, [], [{:fixed_string, 30}])

decode_type("DateTime64(3, 'Asia/Taipei')", nil, [], [])
