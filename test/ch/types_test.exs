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

      assert decode("JSON(max_dynamic_types=10)") == {:json, [max_dynamic_types: 10]}

      assert decode(" JSON ( max_dynamic_paths = 1024, max_dynamic_types = 32 ) ") == :json

      assert decode("""
             JSON(
               max_dynamic_paths = 1024,
               max_dynamic_types = 32,
               "payload)" Map(String, Tuple(UInt8, String)),
               SKIP ignored.path,
               SKIP REGEXP 'tmp_[,()]'
             )
             """) ==
               {:json,
                [
                  typed_paths: [{"payload)", {:map, :string, {:tuple, [:u8, :string]}}}],
                  skip_paths: ["ignored.path"],
                  skip_regexps: ["tmp_[,()]"]
                ]}

      assert decode("Array(JSON(max_dynamic_types=16, max_dynamic_paths=256))") ==
               {:array, {:json, [max_dynamic_types: 16, max_dynamic_paths: 256]}}

      assert decode("Tuple(payload JSON(SKIP `ignored)`), id UInt8)") ==
               {:tuple, [{:json, [skip_paths: ["ignored)"]]}, :u8]}
    end

    test "datetime" do
      assert decode("DateTime") == :datetime
      assert decode("DateTime('UTC')") == {:datetime, "UTC"}
      assert decode("DateTime('Asia/Tokyo')") == {:datetime, "Asia/Tokyo"}
      assert decode(" DateTime ( 'UTC' ) ") == {:datetime, "UTC"}
    end

    test "datetime64" do
      assert decode("DateTime64(3)") == {:datetime64, 3}
      assert decode("DateTime64(3, 'UTC')") == {:datetime64, 3, "UTC"}
      assert decode("DateTime64(3, 'Asia/Tokyo')") == {:datetime64, 3, "Asia/Tokyo"}
      assert decode(" DateTime64 ( 3 , 'Asia/Taipei' ) ") == {:datetime64, 3, "Asia/Taipei"}
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

  describe "JSON options" do
    @json_cases [
      {"JSON(max_dynamic_paths = 10, max_dynamic_types = 3)",
       {:json, [max_dynamic_types: 3, max_dynamic_paths: 10]},
       "JSON(max_dynamic_types=3, max_dynamic_paths=10)"},
      {"JSON(max_dynamic_types=32)", :json, "JSON"},
      {"JSON(b String, `a`.`b` UInt8)", {:json, [typed_paths: [{"a.b", :u8}, {"b", :string}]]},
       "JSON(`a.b` UInt8, b String)"},
      {"JSON(SKIP z, SKIP a.b, SKIP z)", {:json, [skip_paths: ["a.b", "z"]]},
       "JSON(SKIP `a.b`, SKIP z)"},
      {~S"JSON(skip regexp 'tmp_[,()]', SKIP REGEXP 'a\'b')",
       {:json, [skip_regexps: ["a'b", "tmp_[,()]"]]},
       ~S"JSON(SKIP REGEXP 'a\'b', SKIP REGEXP 'tmp_[,()]')"},
      {"JSON(`select` UInt8, `from` UInt8)",
       {:json, [typed_paths: [{"from", :u8}, {"select", :u8}]]},
       "JSON(from UInt8, `select` UInt8)"},
      {"JSON(`skip` UInt8, SKIP `skip`)",
       {:json, [typed_paths: [{"skip", :u8}], skip_paths: ["skip"]]},
       "JSON(`skip` UInt8, SKIP skip)"},
      {"JSON(SKIP z, x UInt8, max_dynamic_types=3, SKIP REGEXP 'q', max_dynamic_paths=7, a String)",
       {:json,
        [
          max_dynamic_types: 3,
          max_dynamic_paths: 7,
          typed_paths: [{"a", :string}, {"x", :u8}],
          skip_paths: ["z"],
          skip_regexps: ["q"]
        ]},
       "JSON(max_dynamic_types=3, max_dynamic_paths=7, a String, x UInt8, SKIP z, SKIP REGEXP 'q')"},
      {~S"JSON(SKIP REGEXP 'a\d\.x')", {:json, [skip_regexps: [~S"a\d\.x"]]},
       ~S"JSON(SKIP REGEXP 'a\\d\\.x')"},
      {~S"JSON(SKIP REGEXP 'a\Nb\ec')", {:json, [skip_regexps: ["ab\ec"]]},
       "JSON(SKIP REGEXP 'ab\ec')"},
      {~S"JSON(`a\d` UInt8)", {:json, [typed_paths: [{~S"a\d", :u8}]]}, ~S"JSON(`a\\d` UInt8)"}
    ]

    test "decode and encode normalize to the canonical form" do
      for {input, expected, canonical} <- @json_cases do
        assert decode(input) == expected
        assert IO.iodata_to_binary(encode(expected)) == canonical
        assert decode(canonical) == expected
      end
    end

    test "hint types are decoded types" do
      assert decode("JSON(a DateTime64(3, 'UTC'), b Decimal(10, 2), c Array(Nullable(String)))") ==
               {:json,
                [
                  typed_paths: [
                    {"a", decode("DateTime64(3, 'UTC')")},
                    {"b", decode("Decimal(10, 2)")},
                    {"c", decode("Array(Nullable(String))")}
                  ]
                ]}
    end

    test "SHOW CREATE TABLE form" do
      padded = "JSON(max_dynamic_types = 3, max_dynamic_paths = 10, a String,     SKIP q)"

      assert decode(padded) ==
               {:json,
                [
                  max_dynamic_types: 3,
                  max_dynamic_paths: 10,
                  typed_paths: [{"a", :string}],
                  skip_paths: ["q"]
                ]}
    end

    test "escapes" do
      assert decode("JSON(SKIP REGEXP 'it''s')") == {:json, [skip_regexps: ["it's"]]}
      assert decode("JSON(`a``b` UInt8)") == {:json, [typed_paths: [{"a`b", :u8}]]}

      assert IO.iodata_to_binary(encode({:json, [typed_paths: [{"a`b", :u8}]]})) ==
               ~S"JSON(`a\`b` UInt8)"
    end

    test "nesting" do
      assert decode("Nullable(JSON(max_dynamic_paths=8))") ==
               {:nullable, {:json, [max_dynamic_paths: 8]}}

      assert decode("Map(String, JSON(SKIP q))") ==
               {:map, :string, {:json, [skip_paths: ["q"]]}}

      for s <- [
            "Nullable(JSON(max_dynamic_paths=8))",
            "Map(String, JSON(SKIP q))",
            "Tuple(JSON(a UInt8), UInt8)",
            "JSON(a JSON(max_dynamic_paths=3, b UInt8))",
            "JSON(a Array(JSON(b UInt8)))"
          ] do
        assert decode(IO.iodata_to_binary(encode(decode(s)))) == decode(s)
      end
    end

    test "hint named like a keyword" do
      assert decode("JSON(skipx UInt8)") == {:json, [typed_paths: [{"skipx", :u8}]]}
    end

    test "unsupported hint types raise" do
      assert_raise ArgumentError, ~r/failed to decode .*Nested/, fn ->
        decode("JSON(a Nested(x UInt8))")
      end

      assert_raise ArgumentError, ~r/failed to decode .*Geometry/, fn ->
        decode("JSON(a Geometry)")
      end
    end

    test "errors" do
      assert_raise ArgumentError, fn -> decode("JSON(foo=1)") end

      assert_raise ArgumentError,
                   ~s[failed to decode "JSON(a UInt8" as ClickHouse type (unexpected end of type while decoding)],
                   fn -> decode("JSON(a UInt8") end

      for input <- [
            "JSON(SKIP REGEXP )",
            "JSON(a UInt8, SKIP REGEXP )",
            "JSON(max_dynamic_types=)"
          ] do
        assert_raise ArgumentError, ~r/failed to decode/, fn -> decode(input) end
      end
    end

    test "json/1" do
      assert Ch.Types.json([]) == :json

      for opts <- [[:foo], [max_dynamic_types: "x"], [skip_paths: "a"], [skip_paths: :x, a: 1]] do
        assert_raise ArgumentError, ~r/invalid JSON option/, fn -> Ch.Types.json(opts) end
      end
    end
  end
end
