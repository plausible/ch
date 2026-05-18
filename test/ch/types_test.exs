defmodule Ch.TypesTest do
  use ExUnit.Case, async: true

  import Ch.Types, only: [decode: 1, encode: 1]

  doctest Ch.Types, import: true

  describe "decode/1" do
    test "scalar types" do
      assert decode("String") == :string

      assert decode("Bool") == :boolean

      assert decode("Int8") == :i8
      assert decode("Int16") == :i16
      assert decode("Int32") == :i32
      assert decode("Int64") == :i64
      assert decode("Int128") == :i128
      assert decode("Int256") == :i256

      assert decode("UInt8") == :u8
      assert decode("UInt16") == :u16
      assert decode("UInt32") == :u32
      assert decode("UInt64") == :u64
      assert decode("UInt128") == :u128
      assert decode("UInt256") == :u256

      assert decode("Float32") == :f32
      assert decode("Float64") == :f64

      assert decode("Date") == :date
      assert decode("DateTime") == :datetime
      assert decode("Date32") == :date32
      assert decode("Time") == :time

      assert decode("UUID") == :uuid

      assert decode("IPv4") == :ipv4
      assert decode("IPv6") == :ipv6

      assert decode("Point") == :point
      assert decode("Ring") == :ring
      assert decode("Polygon") == :polygon
      assert decode("MultiPolygon") == :multipolygon
    end

    test "scalar type with whitespace" do
      assert decode(" String") == :string
      assert decode("String ") == :string
      assert decode(" String ") == :string
    end

    test "array" do
      assert decode("Array(Int8)") == {:array, :i8}
      assert decode("Array(Nothing)") == {:array, :nothing}
      assert decode("Array ( Int8) ") == {:array, :i8}
      assert decode(" Array ( Array ( String ) ) ") == {:array, {:array, :string}}
    end

    test "tuple" do
      assert decode("Tuple(UInt8, UInt8)") == {:tuple, [:u8, :u8]}
      assert decode(" Tuple ( UInt8 , UInt8 ) ") == {:tuple, [:u8, :u8]}

      assert decode(
               " Tuple ( Array( String ) , Tuple ( String , UInt64 ), DateTime, DateTime64(3), FixedString(3) ) "
             ) ==
               {:tuple,
                [
                  {:array, :string},
                  {:tuple, [:string, :u64]},
                  :datetime,
                  {:datetime64, 3},
                  {:fixed_string, 3}
                ]}
    end

    test "named tuple" do
      assert decode("Tuple(a String, b UInt8)") == {:tuple, [:string, :u8]}
      assert decode(" Tuple ( a String , b UInt8 ) ") == {:tuple, [:string, :u8]}

      assert decode(
               " Tuple ( a Array( String ) , t Tuple ( a String , b UInt64 ), d DateTime, d64 DateTime64(3), f FixedString(3) ) "
             ) ==
               {:tuple,
                [
                  {:array, :string},
                  {:tuple, [:string, :u64]},
                  :datetime,
                  {:datetime64, 3},
                  {:fixed_string, 3}
                ]}
    end

    test "variant" do
      assert decode("Variant(UInt64, String, Array(UInt64))") ==
               {:variant, [{:array, :u64}, :string, :u64]}

      assert decode("Variant ( UInt64 , String , Array ( UInt64 ) )") ==
               {:variant, [{:array, :u64}, :string, :u64]}
    end

    test "dynamic" do
      assert decode("Dynamic") == :dynamic
      assert decode(" Dynamic ") == :dynamic
      assert decode(" Dynamic(max_types=10) ") == :dynamic
      assert decode(" Dynamic (       max_types  =10) ") == :dynamic
    end

    test "json" do
      assert decode("JSON") == :json
      assert decode(" JSON ") == :json

      # TODO JSON(...)
    end

    test "datetime" do
      assert decode("DateTime") == :datetime
      assert decode("DateTime('UTC')") == {:datetime, "UTC"}
      assert decode("DateTime('Asia/Tokyo')") == {:datetime, "Asia/Tokyo"}
      assert decode(" DateTime ( 'UTC' ) ") == {:datetime, "UTC"}

      assert decode("DateTime('has\\'quote\\\\slash\\t\\n\\r')") ==
               {:datetime, "has'quote\\slash\t\n\r"}

      assert decode("DateTime('has''quote')") == {:datetime, "has'quote"}
    end

    test "datetime64" do
      assert decode("DateTime64(3)") == {:datetime64, 3}
      assert decode("DateTime64(3, 'UTC')") == {:datetime64, 3, "UTC"}
      assert decode("DateTime64(3, 'Asia/Tokyo')") == {:datetime64, 3, "Asia/Tokyo"}
      assert decode(" DateTime64 ( 3 , 'Asia/Taipei' ) ") == {:datetime64, 3, "Asia/Taipei"}

      assert decode("DateTime64(3, 'has\\'quote\\\\slash\\t\\n\\r')") ==
               {:datetime64, 3, "has'quote\\slash\t\n\r"}

      assert decode("DateTime64(3, 'has''quote')") == {:datetime64, 3, "has'quote"}
    end

    test "time64" do
      assert decode("Time64(3)") == {:time64, 3}
      assert decode(" Time64(    5)") == {:time64, 5}
    end

    test "fixed_string" do
      assert decode("FixedString(3)") == {:fixed_string, 3}
      assert decode("FixedString(16)") == {:fixed_string, 16}
    end

    test "map" do
      assert decode("Map(String, UInt64)") == {:map, :string, :u64}
      assert decode(" Map( String , Array ( String ) ) ") == {:map, :string, {:array, :string}}
    end

    test "nullable" do
      assert decode(" Nullable ( String ) ") == {:nullable, :string}
    end

    test "low cardinality" do
      assert decode(" LowCardinality (String)") == {:low_cardinality, :string}
    end

    test "enum" do
      assert decode("Enum8('hello' = 1, 'world' = 2)") == {:enum8, [{"hello", 1}, {"world", 2}]}
      assert decode("Enum8('é€𐍈' = 1)") == {:enum8, [{"é€𐍈", 1}]}

      assert decode("Enum8('has\\'quote' = 1, 'has''quote' = 2, 'has\\\\slash' = 3)") ==
               {:enum8, [{"has'quote", 1}, {"has'quote", 2}, {"has\\slash", 3}]}

      assert decode("Enum8('tabs\\tnewlines\\nreturns\\r' = 1)") ==
               {:enum8, [{"tabs\tnewlines\nreturns\r", 1}]}

      assert decode("Enum16('hello' = -1, 'world' = 2)") ==
               {:enum16, [{"hello", -1}, {"world", 2}]}

      assert decode("Enum8('hello'=1,'world'=2)") == {:enum8, [{"hello", 1}, {"world", 2}]}

      assert decode(" Enum8 ( 'hello' = 1 , 'world' = 2 ) ") ==
               {:enum8, [{"hello", 1}, {"world", 2}]}

      assert decode("Enum8('enum8_min' = -128, 'enum8_zero' = 0, 'enum8_max' = 127)") ==
               {:enum8, [{"enum8_min", -128}, {"enum8_zero", 0}, {"enum8_max", 127}]}

      assert decode("Enum16('enum16_min' = -32768, 'enum16_zero' = 0, 'enum16_max' = 32767)") ==
               {:enum16, [{"enum16_min", -32768}, {"enum16_zero", 0}, {"enum16_max", 32767}]}
    end

    test "simple aggregate function" do
      assert decode("SimpleAggregateFunction(any, UInt64)") ==
               {:simple_aggregate_function, "any", :u64}

      assert decode("SimpleAggregateFunction( any , Map ( String , UInt64 ))") ==
               {:simple_aggregate_function, "any", {:map, :string, :u64}}
    end

    test "newlines" do
      assert decode("""
             Tuple(
               String,
               Array(
                 UInt64
               )
             )
             """) == {:tuple, [:string, {:array, :u64}]}
    end

    test "incomplete input" do
      assert_raise ArgumentError,
                   ~s[failed to decode "Tuple(String, Array(" as ClickHouse type (unexpected end of type while decoding)],
                   fn -> decode("Tuple(String, Array(") end
    end

    test "unexpected character" do
      assert_raise ArgumentError,
                   ~s[failed to decode "Int8$" as ClickHouse type (unexpected character "$" in type while decoding)],
                   fn -> decode("Int8$") end
    end
  end

  describe "encode/1" do
    test "rejects empty enum mappings" do
      assert_raise ArgumentError, "Enum8 requires at least one mapping", fn ->
        encode({:enum8, []})
      end

      assert_raise ArgumentError, "Enum16 requires at least one mapping", fn ->
        encode({:enum16, []})
      end
    end
  end
end

defmodule Ch.TypesPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Ch.Types, only: [decode: 1, encode: 1]

  @scalar_types [
    :string,
    :boolean,
    :i8,
    :i16,
    :i32,
    :i64,
    :i128,
    :i256,
    :u8,
    :u16,
    :u32,
    :u64,
    :u128,
    :u256,
    :f32,
    :f64,
    :date,
    :datetime,
    :date32,
    :time,
    :uuid,
    :ipv4,
    :ipv6,
    :point,
    :ring,
    :polygon,
    :multipolygon,
    :nothing,
    :json,
    :dynamic
  ]

  describe "encode/decode properties" do
    property "quoted string arguments round-trip through escaping" do
      check all string <- quoted_string_gen(),
                max_runs: 300 do
        assert string
               |> then(&{:datetime, &1})
               |> encode()
               |> IO.iodata_to_binary()
               |> decode() == {:datetime, string}

        assert string
               |> then(&{:datetime64, 3, &1})
               |> encode()
               |> IO.iodata_to_binary()
               |> decode() == {:datetime64, 3, string}

        assert [{"enum", 1}, {string, 2}]
               |> then(&{:enum8, &1})
               |> encode()
               |> IO.iodata_to_binary()
               |> decode() == {:enum8, [{"enum", 1}, {string, 2}]}
      end
    end

    property "generated types round-trip through encode/decode" do
      check all type <- type_gen(),
                max_runs: 500 do
        assert type |> encode() |> IO.iodata_to_binary() |> decode() == normalize_type(type)
      end
    end

    property "generated types decode with arbitrary whitespace" do
      check all type <- type_gen(),
                whitespace <- whitespace_gen(),
                max_runs: 500 do
        assert type |> render_type(whitespace) |> decode() == normalize_decoded_type(type)
      end
    end

    property "decode accepts scalar types with arbitrary surrounding whitespace" do
      check all type <- member_of(@scalar_types),
                left <- whitespace_gen(),
                right <- whitespace_gen() do
        encoded = IO.iodata_to_binary([left, encode(type), right])

        assert decode(encoded) == type
      end
    end

    property "decode rejects non-empty junk after a valid type" do
      check all type <- type_gen() |> filter(&(&1 != :json)),
                junk <- junk_suffix_gen() do
        encoded = type |> encode() |> IO.iodata_to_binary()

        assert_raise ArgumentError, ~r/unexpected character/, fn ->
          decode(encoded <> junk)
        end
      end
    end

    property "decode rejects truncated parameterized types" do
      check all type <- parameterized_type_gen() do
        encoded = type |> encode() |> IO.iodata_to_binary()
        size = byte_size(encoded)
        truncated = binary_part(encoded, 0, size - 1)

        assert_raise ArgumentError, fn ->
          decode(truncated)
        end
      end
    end
  end

  defp type_gen do
    tree(member_of(@scalar_types), &parameterized_type_gen/1)
  end

  defp parameterized_type_gen do
    type_gen()
    |> bind(fn
      atom when atom in @scalar_types ->
        parameterized_type_gen(constant(atom))

      type ->
        constant(type)
    end)
  end

  defp parameterized_type_gen(type_gen) do
    gen all kind <-
              member_of([
                :array,
                :tuple,
                :variant,
                :map,
                :nullable,
                :low_cardinality,
                :fixed_string,
                :datetime,
                :datetime64,
                :time64,
                :decimal,
                :decimal_sized,
                :enum,
                :simple_aggregate_function
              ]) do
      kind
    end
    |> bind(fn
      :array ->
        map(type_gen, &{:array, &1})

      :tuple ->
        map(list_of(type_gen, max_length: 6), &{:tuple, &1})

      :variant ->
        map(list_of(type_gen, min_length: 1, max_length: 6), &{:variant, &1})

      :map ->
        map({type_gen, type_gen}, fn {key_type, value_type} -> {:map, key_type, value_type} end)

      :nullable ->
        map(type_gen, &{:nullable, &1})

      :low_cardinality ->
        map(type_gen, &{:low_cardinality, &1})

      :fixed_string ->
        map(integer(1..1024), &{:fixed_string, &1})

      :datetime ->
        map(timezone_gen(), &{:datetime, &1})

      :datetime64 ->
        one_of([
          map(integer(0..9), &{:datetime64, &1}),
          map({integer(0..9), timezone_gen()}, fn {precision, timezone} ->
            {:datetime64, precision, timezone}
          end)
        ])

      :time64 ->
        map(integer(0..9), &{:time64, &1})

      :decimal ->
        gen all precision <- integer(1..76),
                scale <- integer(0..precision) do
          {:decimal, precision, scale}
        end

      :decimal_sized ->
        gen all type <- member_of([:decimal32, :decimal64, :decimal128, :decimal256]),
                scale <- integer(0..9) do
          {type, scale}
        end

      :enum ->
        gen all type <- member_of([:enum8, :enum16]),
                mapping <- enum_mapping_gen(type) do
          {type, mapping}
        end

      :simple_aggregate_function ->
        gen all function <- identifier_gen(),
                type <- type_gen do
          {:simple_aggregate_function, function, type}
        end
    end)
  end

  defp enum_mapping_gen(:enum8) do
    uniq_list_of({enum_name_gen(), integer(-128..127)}, min_length: 1, max_length: 8)
  end

  defp enum_mapping_gen(:enum16) do
    uniq_list_of({enum_name_gen(), integer(-32768..32767)}, min_length: 1, max_length: 8)
  end

  defp timezone_gen do
    one_of([
      member_of(["UTC", "Europe/Vienna", "Asia/Tokyo", "America/New_York", "Etc/GMT-3"]),
      quoted_string_gen()
    ])
  end

  defp identifier_gen do
    gen all first <- string([?a..?z, ?A..?Z], length: 1),
            rest <- string([?a..?z, ?A..?Z, ?0..?9, ?_], max_length: 12) do
      first <> rest
    end
  end

  defp enum_name_gen do
    quoted_string_gen()
  end

  defp quoted_string_gen do
    string(
      [
        ?a..?z,
        ?A..?Z,
        ?0..?9,
        ?_,
        ?\s,
        ?\t,
        ?\n,
        ?\r,
        ?-,
        ?.,
        ?/,
        ?',
        ?\\,
        ?é,
        ?€,
        ?𐍈
      ],
      min_length: 1,
      max_length: 20
    )
  end

  defp whitespace_gen do
    string([?\s, ?\t, ?\n, ?\r], max_length: 6)
  end

  defp junk_suffix_gen do
    string([?$], min_length: 1, max_length: 4)
  end

  defp normalize_type({:variant, types}) do
    {:variant,
     Enum.sort_by(Enum.map(types, &normalize_type/1), fn t -> IO.iodata_to_binary(encode(t)) end)}
  end

  defp normalize_type({:decimal32, scale}), do: {:decimal, 9, scale}
  defp normalize_type({:decimal64, scale}), do: {:decimal, 18, scale}
  defp normalize_type({:decimal128, scale}), do: {:decimal, 38, scale}
  defp normalize_type({:decimal256, scale}), do: {:decimal, 76, scale}
  defp normalize_type({:array, type}), do: {:array, normalize_type(type)}
  defp normalize_type({:tuple, types}), do: {:tuple, Enum.map(types, &normalize_type/1)}

  defp normalize_type({:map, key_type, value_type}),
    do: {:map, normalize_type(key_type), normalize_type(value_type)}

  defp normalize_type({:nullable, type}), do: {:nullable, normalize_type(type)}
  defp normalize_type({:low_cardinality, type}), do: {:low_cardinality, normalize_type(type)}

  defp normalize_type({:simple_aggregate_function, function, type}) do
    {:simple_aggregate_function, function, normalize_type(type)}
  end

  defp normalize_type(type), do: type

  defp normalize_decoded_type({:variant, types}) do
    {:variant,
     Enum.sort_by(Enum.map(types, &normalize_decoded_type/1), fn t ->
       IO.iodata_to_binary(encode(t))
     end)}
  end

  defp normalize_decoded_type({:array, type}), do: {:array, normalize_decoded_type(type)}

  defp normalize_decoded_type({:tuple, types}),
    do: {:tuple, Enum.map(types, &normalize_decoded_type/1)}

  defp normalize_decoded_type({:map, key_type, value_type}),
    do: {:map, normalize_decoded_type(key_type), normalize_decoded_type(value_type)}

  defp normalize_decoded_type({:nullable, type}), do: {:nullable, normalize_decoded_type(type)}

  defp normalize_decoded_type({:low_cardinality, type}),
    do: {:low_cardinality, normalize_decoded_type(type)}

  defp normalize_decoded_type({:simple_aggregate_function, function, type}) do
    {:simple_aggregate_function, function, normalize_decoded_type(type)}
  end

  defp normalize_decoded_type(type), do: type

  defp render_type(type, whitespace) do
    [whitespace, render_type!(type, whitespace), whitespace] |> IO.iodata_to_binary()
  end

  defp render_type!(type, _whitespace) when type in @scalar_types, do: encode(type)

  defp render_type!({:fixed_string, n}, whitespace),
    do: ["FixedString", whitespace, ?(, whitespace, Integer.to_string(n), whitespace, ?)]

  defp render_type!({:time64, p}, whitespace),
    do: ["Time64", whitespace, ?(, whitespace, Integer.to_string(p), whitespace, ?)]

  defp render_type!({:decimal, p, s}, whitespace),
    do: [
      "Decimal",
      whitespace,
      ?(,
      whitespace,
      Integer.to_string(p),
      whitespace,
      ?,,
      whitespace,
      Integer.to_string(s),
      whitespace,
      ?)
    ]

  defp render_type!({:decimal32, s}, whitespace),
    do: render_decimal_sized("Decimal32", s, whitespace)

  defp render_type!({:decimal64, s}, whitespace),
    do: render_decimal_sized("Decimal64", s, whitespace)

  defp render_type!({:decimal128, s}, whitespace),
    do: render_decimal_sized("Decimal128", s, whitespace)

  defp render_type!({:decimal256, s}, whitespace),
    do: render_decimal_sized("Decimal256", s, whitespace)

  defp render_type!({:datetime, timezone}, whitespace),
    do: ["DateTime", whitespace, ?(, whitespace, ?', render_string(timezone), ?', whitespace, ?)]

  defp render_type!({:datetime64, p}, whitespace),
    do: ["DateTime64", whitespace, ?(, whitespace, Integer.to_string(p), whitespace, ?)]

  defp render_type!({:datetime64, p, timezone}, whitespace) do
    [
      "DateTime64",
      whitespace,
      ?(,
      whitespace,
      Integer.to_string(p),
      whitespace,
      ?,,
      whitespace,
      ?',
      render_string(timezone),
      ?',
      whitespace,
      ?)
    ]
  end

  defp render_type!({:array, type}, whitespace), do: render_unary("Array", type, whitespace)
  defp render_type!({:nullable, type}, whitespace), do: render_unary("Nullable", type, whitespace)

  defp render_type!({:low_cardinality, type}, whitespace),
    do: render_unary("LowCardinality", type, whitespace)

  defp render_type!({:tuple, types}, whitespace) do
    ["Tuple", whitespace, ?(, whitespace, render_types(types, whitespace), whitespace, ?)]
  end

  defp render_type!({:variant, types}, whitespace) do
    ["Variant", whitespace, ?(, whitespace, render_types(types, whitespace), whitespace, ?)]
  end

  defp render_type!({:map, key_type, value_type}, whitespace) do
    [
      "Map",
      whitespace,
      ?(,
      whitespace,
      render_type!(key_type, whitespace),
      whitespace,
      ?,,
      whitespace,
      render_type!(value_type, whitespace),
      whitespace,
      ?)
    ]
  end

  defp render_type!({:enum8, mapping}, whitespace), do: render_enum("Enum8", mapping, whitespace)

  defp render_type!({:enum16, mapping}, whitespace),
    do: render_enum("Enum16", mapping, whitespace)

  defp render_type!({:simple_aggregate_function, function, type}, whitespace) do
    [
      "SimpleAggregateFunction",
      whitespace,
      ?(,
      whitespace,
      function,
      whitespace,
      ?,,
      whitespace,
      render_type!(type, whitespace),
      whitespace,
      ?)
    ]
  end

  defp render_decimal_sized(name, scale, whitespace) do
    [name, whitespace, ?(, whitespace, Integer.to_string(scale), whitespace, ?)]
  end

  defp render_unary(name, type, whitespace) do
    [name, whitespace, ?(, whitespace, render_type!(type, whitespace), whitespace, ?)]
  end

  defp render_types(types, whitespace) do
    types
    |> Enum.map(&render_type!(&1, whitespace))
    |> Enum.intersperse([whitespace, ?,, whitespace])
  end

  defp render_enum(name, mapping, whitespace) do
    [name, whitespace, ?(, whitespace, render_mapping(mapping, whitespace), whitespace, ?)]
  end

  defp render_mapping(mapping, whitespace) do
    mapping
    |> Enum.map(fn {key, value} ->
      [?', render_string(key), ?', whitespace, ?=, whitespace, Integer.to_string(value)]
    end)
    |> Enum.intersperse([whitespace, ?,, whitespace])
  end

  defp render_string(string) do
    string
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "\\'")
    |> String.replace("\t", "\\t")
    |> String.replace("\n", "\\n")
    |> String.replace("\r", "\\r")
  end
end
