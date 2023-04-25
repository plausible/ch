defmodule Ch.TypesTest do
  use ExUnit.Case, async: true
  import Ch.Types, only: [decode: 1]
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

      assert decode("Enum16('hello' = -1, 'world' = 2)") ==
               {:enum16, [{"hello", -1}, {"world", 2}]}

      assert decode("Enum8('hello'=1,'world'=2)") == {:enum8, [{"hello", 1}, {"world", 2}]}

      assert decode(" Enum8 ( 'hello' = 1 , 'world' = 2 ) ") ==
               {:enum8, [{"hello", 1}, {"world", 2}]}
    end

    test "simple aggregate function" do
      assert decode("SimpleAggregateFunction(any, UInt64)") ==
               {:simple_aggregate_function, "any", :u64}

      assert decode("SimpleAggregateFunction( any , Map ( String , UInt64 ))") ==
               {:simple_aggregate_function, "any", {:map, :string, :u64}}
    end
  end
end
