defmodule Ch.RowBinaryArrayTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary
  import Bitwise

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "array params round-trip through ClickHouse across integer widths", %{pool: pool} do
    check all {type, values, expected} <- integer_array_param() do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [expected]
             ]
    end
  end

  property "array params round-trip through ClickHouse across element kinds", %{pool: pool} do
    check all {type, values, expected} <- array_param() do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [expected]
             ]
    end
  end

  test "array params cover every integer width through ClickHouse", %{pool: pool} do
    for {type, values} <- integer_width_examples() do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [values]
             ]
    end
  end

  test "array params cover representative element kinds through ClickHouse", %{pool: pool} do
    uuid = "417ddc5d-e556-4d27-95dd-a34d84e46a50"

    cases = [
      {"Bool", [true, false], [true, false]},
      {"String", ["", "hello", "tabs\tand\nlines"], ["", "hello", "tabs\tand\nlines"]},
      {"FixedString(3)", ["", "a", "abc"], [<<0, 0, 0>>, "a" <> <<0, 0>>, "abc"]},
      {"Nullable(String)", ["a", nil, "bc"], ["a", nil, "bc"]},
      {"Decimal(18, 4)", [Decimal.new("1.23"), Decimal.new("-4.56789")],
       [Decimal.new("1.2300"), Decimal.new("-4.5678")]},
      {"Float32", [-1, 0, 3], [-1.0, 0.0, 3.0]},
      {"Float64", [-1, 0, 3], [-1.0, 0.0, 3.0]},
      {"Date", [~D[1970-01-01], ~D[2024-02-29]], [~D[1970-01-01], ~D[2024-02-29]]},
      {"Date32", [~D[1960-01-01], ~D[2100-01-01]], [~D[1960-01-01], ~D[2100-01-01]]},
      {"DateTime('UTC')", [~U[2024-01-02 03:04:05Z]], [~U[2024-01-02 03:04:05Z]]},
      {"DateTime64(6, 'UTC')", [~U[2024-01-02 03:04:05.123456Z]],
       [~U[2024-01-02 03:04:05.123456Z]]},
      {"UUID", [uuid], [uuid_to_binary(uuid)]},
      {"IPv4", ["127.0.0.1", "192.168.1.1"], [{127, 0, 0, 1}, {192, 168, 1, 1}]},
      {"IPv6", ["::1", "2001:4860:4860::8888"],
       [{0, 0, 0, 0, 0, 0, 0, 1}, {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}]},
      {"Enum8('hello' = 1, 'world' = 2)", ["hello", "world"], ["hello", "world"]},
      {"Array(UInt8)", [[1, 2], [], [3]], [[1, 2], [], [3]]},
      {"Tuple(String, UInt8)", [{"one", 1}, {"two", 2}], [{"one", 1}, {"two", 2}]},
      {"Map(String, UInt8)", [%{"a" => 1}, %{"b" => 2, "c" => 3}],
       [%{"a" => 1}, %{"b" => 2, "c" => 3}]}
    ]

    for {type, values, expected} <- cases do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [expected]
             ]
    end
  end

  test "arrays of scalar types inserted as RowBinary round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_array_scalars (
      ints Array(Int16),
      uints Array(UInt64),
      floats32 Array(Float32),
      floats64 Array(Float64),
      bools Array(Bool),
      strings Array(String),
      fixed_strings Array(FixedString(3)),
      nullable_strings Array(Nullable(String)),
      decimals Array(Decimal(18, 4)),
      dates Array(Date),
      date32s Array(Date32),
      datetimes Array(DateTime('UTC')),
      datetime64s Array(DateTime64(6, 'UTC'))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_array_scalars") end)

    rows = [
      [
        [-2, -1, 0, 1, 2],
        [0, 1, 18_446_744_073_709_551_615],
        [-1.5, 0.0, 3.25],
        [-1.5, 0.0, 3.25],
        [true, false, true],
        ["", "hello", "tabs\tand\nlines"],
        ["", "a", "abc"],
        ["a", nil, "bc", nil],
        [Decimal.new("1.2300"), Decimal.new("-4.5678")],
        [~D[1970-01-01], ~D[2024-02-29]],
        [~D[1960-01-01], ~D[2100-01-01]],
        [~U[2024-01-02 03:04:05Z]],
        [~U[2024-01-02 03:04:05.123456Z]]
      ],
      [
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        []
      ]
    ]

    types = [
      "Array(Int16)",
      "Array(UInt64)",
      "Array(Float32)",
      "Array(Float64)",
      "Array(Bool)",
      "Array(String)",
      "Array(FixedString(3))",
      "Array(Nullable(String))",
      "Array(Decimal(18, 4))",
      "Array(Date)",
      "Array(Date32)",
      "Array(DateTime('UTC'))",
      "Array(DateTime64(6, 'UTC'))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_array_scalars FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_array_scalars").rows == [
             [
               [-2, -1, 0, 1, 2],
               [0, 1, 18_446_744_073_709_551_615],
               [-1.5, 0.0, 3.25],
               [-1.5, 0.0, 3.25],
               [true, false, true],
               ["", "hello", "tabs\tand\nlines"],
               [<<0, 0, 0>>, "a" <> <<0, 0>>, "abc"],
               ["a", nil, "bc", nil],
               [Decimal.new("1.2300"), Decimal.new("-4.5678")],
               [~D[1970-01-01], ~D[2024-02-29]],
               [~D[1960-01-01], ~D[2100-01-01]],
               [~U[2024-01-02 03:04:05Z]],
               [~U[2024-01-02 03:04:05.123456Z]]
             ],
             [[], [], [], [], [], [], [], [], [], [], [], [], []]
           ]
  end

  test "arrays of structured types inserted as RowBinary round-trip through ClickHouse", %{
    pool: pool
  } do
    Help.query!("""
    CREATE TABLE row_binary_array_structured (
      nested Array(Array(UInt8)),
      tuples Array(Tuple(String, UInt8)),
      maps Array(Map(String, UInt8)),
      enums Array(Enum8('hello' = 1, 'world' = 2)),
      uuids Array(UUID),
      ipv4s Array(IPv4),
      ipv6s Array(IPv6),
      points Array(Point)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_array_structured") end)

    uuid1 = "417ddc5d-e556-4d27-95dd-a34d84e46a50" |> uuid_to_binary()
    uuid2 = "00010203-0405-0607-0809-0a0b0c0d0e0f" |> uuid_to_binary()

    row = [
      [[1, 2], [], [3]],
      [{"one", 1}, {"two", 2}],
      [%{"a" => 1}, %{"b" => 2, "c" => 3}],
      ["hello", "world", "hello"],
      [uuid1, uuid2],
      [{127, 0, 0, 1}, {192, 168, 1, 1}],
      [{0, 0, 0, 0, 0, 0, 0, 1}, {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}],
      [{10.0, 20.0}, {-5.5, 4.25}]
    ]

    types = [
      "Array(Array(UInt8))",
      "Array(Tuple(String, UInt8))",
      "Array(Map(String, UInt8))",
      "Array(Enum8('hello' = 1, 'world' = 2))",
      "Array(UUID)",
      "Array(IPv4)",
      "Array(IPv6)",
      "Array(Point)"
    ]

    rowbinary = RowBinary.encode_rows([row], types)
    Ch.query!(pool, ["INSERT INTO row_binary_array_structured FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_array_structured").rows == [row]
  end

  test "ClickHouse rejects invalid array parameter elements", %{pool: pool} do
    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:Array(UInt8)}", %{"value" => [1, "bad"]})

    assert message =~ "UInt8"
  end

  defp integer_array_param do
    one_of([
      typed_array("Int8", integer(-128..127)),
      typed_array("Int16", integer(-32_768..32_767)),
      typed_array("Int32", integer(-2_147_483_648..2_147_483_647)),
      typed_array("Int64", integer(-9_007_199_254_740_992..9_007_199_254_740_991)),
      typed_array("Int128", signed_integer(128)),
      typed_array("Int256", signed_integer(256)),
      typed_array("UInt8", integer(0..255)),
      typed_array("UInt16", integer(0..65_535)),
      typed_array("UInt32", integer(0..4_294_967_295)),
      typed_array("UInt64", integer(0..9_007_199_254_740_991)),
      typed_array("UInt128", unsigned_integer(128)),
      typed_array("UInt256", unsigned_integer(256))
    ])
  end

  defp integer_width_examples do
    [
      {"Int8", [-128, 0, 127]},
      {"Int16", [-32_768, 0, 32_767]},
      {"Int32", [-2_147_483_648, 0, 2_147_483_647]},
      {"Int64", [-9_223_372_036_854_775_808, 0, 9_223_372_036_854_775_807]},
      {"Int128", [-(1 <<< 127), 0, (1 <<< 127) - 1]},
      {"Int256", [-(1 <<< 255), 0, (1 <<< 255) - 1]},
      {"UInt8", [0, 255]},
      {"UInt16", [0, 65_535]},
      {"UInt32", [0, 4_294_967_295]},
      {"UInt64", [0, 18_446_744_073_709_551_615]},
      {"UInt128", [0, (1 <<< 128) - 1]},
      {"UInt256", [0, (1 <<< 256) - 1]}
    ]
  end

  defp array_param do
    one_of([
      typed_array("Bool", boolean()),
      typed_array("String", safe_string()),
      fixed_string_array(),
      typed_array("Nullable(String)", one_of([constant(nil), safe_string()])),
      decimal_array(),
      typed_array("Float32", integer(-10_000..10_000), &Enum.map(&1, fn n -> n * 1.0 end)),
      typed_array("Float64", integer(-10_000..10_000), &Enum.map(&1, fn n -> n * 1.0 end)),
      typed_array("Date", date_gen()),
      typed_array("Date32", date32_gen()),
      typed_array("DateTime('UTC')", utc_datetime()),
      typed_array("DateTime64(6, 'UTC')", utc_datetime64()),
      uuid_array(),
      typed_array(
        "IPv4",
        ipv4_text(),
        &Enum.map(&1, fn text ->
          text |> to_charlist() |> :inet.parse_ipv4strict_address() |> elem(1)
        end)
      ),
      typed_array(
        "IPv6",
        ipv6_text(),
        &Enum.map(&1, fn text ->
          text |> to_charlist() |> :inet.parse_ipv6strict_address() |> elem(1)
        end)
      ),
      typed_array("Enum8('hello' = 1, 'world' = 2)", member_of(["hello", "world"])),
      typed_array("Array(UInt8)", list_of(integer(0..255), max_length: 4)),
      typed_array("Tuple(String, UInt8)", tuple_param()),
      typed_array("Map(String, UInt8)", map_of(safe_string(), integer(0..255), max_length: 4))
    ])
  end

  defp typed_array(type, generator, expected_fun \\ & &1) do
    gen all values <- list_of(generator, max_length: 4) do
      {type, values, expected_fun.(values)}
    end
  end

  defp fixed_string_array do
    gen all size <- integer(1..8),
            values <- list_of(string(:alphanumeric, max_length: size), max_length: 4) do
      expected =
        Enum.map(values, fn value -> value <> :binary.copy(<<0>>, size - byte_size(value)) end)

      {"FixedString(#{size})", values, expected}
    end
  end

  defp decimal_array do
    gen all values <- list_of(decimal_gen(), max_length: 4) do
      {"Decimal(18, 4)", values, Enum.map(values, &Decimal.round(&1, 4))}
    end
  end

  defp uuid_array do
    gen all values <- list_of(uuid_param(), max_length: 4) do
      expected = Enum.map(values, fn {_text, binary} -> binary end)
      {"UUID", Enum.map(values, fn {text, _binary} -> text end), expected}
    end
  end

  defp tuple_param do
    gen all string <- safe_string(),
            n <- integer(0..255) do
      {string, n}
    end
  end

  defp signed_integer(bits) do
    gen all unsigned <- unsigned_integer(bits) do
      signed_limit = 1 <<< (bits - 1)
      if unsigned >= signed_limit, do: unsigned - (1 <<< bits), else: unsigned
    end
  end

  defp unsigned_integer(bits) do
    gen all bytes <- binary(length: div(bits, 8)) do
      :binary.decode_unsigned(bytes, :little)
    end
  end

  defp decimal_gen do
    gen all sign <- member_of([1, -1]),
            coef <- integer(0..999_999_999),
            exp <- integer(-4..4) do
      Decimal.new(sign, coef, exp)
    end
  end

  defp uuid_param do
    gen all bytes <- binary(length: 16) do
      <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2),
        e::binary-size(6)>> = bytes

      uuid =
        [a, b, c, d, e]
        |> Enum.map_join("-", &Base.encode16(&1, case: :lower))

      {uuid, bytes}
    end
  end

  defp uuid_to_binary(uuid) do
    uuid
    |> String.replace("-", "")
    |> Base.decode16!(case: :lower)
  end

  defp ipv4_text do
    gen all a <- integer(0..255),
            b <- integer(0..255),
            c <- integer(0..255),
            d <- integer(0..255) do
      "#{a}.#{b}.#{c}.#{d}"
    end
  end

  defp ipv6_text do
    member_of(["::1", "2001:4860:4860::8888", "2606:4700:4700::1111"])
  end

  defp date_gen do
    gen all days <- integer(0..20_000) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp date32_gen do
    gen all days <- integer(-25_567..120_529) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp utc_datetime do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59) do
      DateTime.new!(date, Time.new!(hour, minute, second), "Etc/UTC")
    end
  end

  defp utc_datetime64 do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      DateTime.new!(date, Time.new!(hour, minute, second, {microsecond, 6}), "Etc/UTC")
    end
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
