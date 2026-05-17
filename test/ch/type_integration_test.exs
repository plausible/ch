defmodule Ch.TypeIntegrationTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "integer params round-trip across ClickHouse integer widths", %{pool: pool} do
    check all {type, value} <- integer_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[value]]
    end
  end

  property "fixed string params are padded to their declared size", %{pool: pool} do
    check all {size, value} <- fixed_string_param() do
      padding = :binary.copy(<<0>>, size - byte_size(value))

      assert Ch.query!(pool, "SELECT {value:FixedString(#{size})}", %{"value" => value}).rows ==
               [[value <> padding]]
    end
  end

  property "decimal params preserve Decimal(18, 4) scale", %{pool: pool} do
    check all value <- decimal_param() do
      assert Ch.query!(pool, "SELECT {value:Decimal(18, 4)}", %{"value" => value}).rows ==
               [[Decimal.round(value, 4)]]
    end
  end

  property "uuid params accept canonical text and decode to 16 bytes", %{pool: pool} do
    check all {uuid_text, uuid_bin} <- uuid_param() do
      assert Ch.query!(pool, "SELECT {value:UUID}, toString({value:UUID})", %{
               "value" => uuid_text
             }).rows == [[uuid_bin, String.downcase(uuid_text)]]
    end
  end

  property "DateTime64 UTC params preserve microseconds", %{pool: pool} do
    check all dt <- utc_datetime64() do
      assert Ch.query!(pool, "SELECT {value:DateTime64(6, 'UTC')}", %{"value" => dt}).rows ==
               [[dt]]
    end
  end

  property "map and tuple params round-trip", %{pool: pool} do
    check all map <- map_of(safe_string(), integer(0..255), max_length: 8),
              tuple <- tuple_param() do
      assert Ch.query!(pool, "SELECT {map:Map(String, UInt8)}, {tuple:Tuple(Int8, String)}", %{
               "map" => map,
               "tuple" => tuple
             }).rows == [[map, tuple]]
    end
  end

  test "fixed strings", %{pool: pool} do
    assert Ch.query!(
             pool,
             "SELECT {empty:FixedString(2)}, {one:FixedString(2)}, {two:FixedString(2)}",
             %{
               "empty" => "",
               "one" => "a",
               "two" => "aa"
             }
           ).rows == [[<<0, 0>>, "a" <> <<0>>, "aa"]]

    Help.query!("CREATE TABLE type_integration_fixed_string(a FixedString(3)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_fixed_string") end)

    rowbinary = RowBinary.encode_rows([[""], ["a"], ["aa"], ["aaa"]], ["FixedString(3)"])
    Ch.query!(pool, ["INSERT INTO type_integration_fixed_string FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM type_integration_fixed_string").rows == [
             [<<0, 0, 0>>],
             ["a" <> <<0, 0>>],
             ["aa" <> <<0>>],
             ["aaa"]
           ]
  end

  test "decimals", %{pool: pool} do
    assert Ch.query!(pool, """
           SELECT
             toDecimal32(2, 4),
             toDecimal64(2, 4),
             toDecimal128(2, 4),
             toDecimal256(2, 4)
           """).rows == [
             [
               Decimal.new("2.0000"),
               Decimal.new("2.0000"),
               Decimal.new("2.0000"),
               Decimal.new("2.0000")
             ]
           ]

    Help.query!("CREATE TABLE type_integration_decimal(d Decimal32(4)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_decimal") end)

    rowbinary =
      RowBinary.encode_rows(
        [[Decimal.new("2.66")], [Decimal.new("2.6666")], [Decimal.new("2.66666")]],
        ["Decimal32(4)"]
      )

    Ch.query!(pool, ["INSERT INTO type_integration_decimal FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM type_integration_decimal").rows == [
             [Decimal.new("2.6600")],
             [Decimal.new("2.6666")],
             [Decimal.new("2.6667")]
           ]
  end

  test "booleans", %{pool: pool} do
    Help.query!("CREATE TABLE type_integration_bool(a Int64, b Bool) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_bool") end)

    Ch.query!(pool, "INSERT INTO type_integration_bool VALUES (1, true), (2, 0), (5, 2)")

    rowbinary = RowBinary.encode_rows([[3, true], [4, false]], ["Int64", "Bool"])
    Ch.query!(pool, ["INSERT INTO type_integration_bool FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT *, a * b FROM type_integration_bool ORDER BY a").rows == [
             [1, true, 1],
             [2, false, 0],
             [3, true, 3],
             [4, false, 0],
             [5, true, 5]
           ]
  end

  property "Bool values inserted as RowBinary round-trip", %{pool: pool} do
    Help.query!("CREATE TABLE type_integration_bool_property(id UInt8, b Bool) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_bool_property") end)

    check all rows <- bool_rows() do
      Ch.query!(pool, "TRUNCATE TABLE type_integration_bool_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "Bool"])

      Ch.query!(pool, [
        "INSERT INTO type_integration_bool_property FORMAT RowBinary\n" | rowbinary
      ])

      assert Ch.query!(pool, "SELECT * FROM type_integration_bool_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "uuid", %{pool: pool} do
    uuid = "417ddc5d-e556-4d27-95dd-a34d84e46a50"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!(case: :lower)

    assert Ch.query!(pool, "SELECT {uuid:UUID}, toString({uuid:UUID})", %{"uuid" => uuid}).rows ==
             [[uuid_bin, uuid]]

    Help.query!("CREATE TABLE type_integration_uuid(x UUID, y String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_uuid") end)

    Ch.query!(pool, "INSERT INTO type_integration_uuid SELECT generateUUIDv4(), 'Example 1'")
    Ch.query!(pool, "INSERT INTO type_integration_uuid(y) VALUES ('Example 2')")

    rowbinary = RowBinary.encode_rows([[uuid_bin, "Example 3"]], ["UUID", "String"])
    Ch.query!(pool, ["INSERT INTO type_integration_uuid(x, y) FORMAT RowBinary\n" | rowbinary])

    assert [
             [generated_uuid, "Example 1"],
             [<<0::128>>, "Example 2"],
             [^uuid_bin, "Example 3"]
           ] = Ch.query!(pool, "SELECT * FROM type_integration_uuid ORDER BY y").rows

    assert byte_size(generated_uuid) == 16
  end

  test "enum8", %{pool: pool} do
    Help.query!(
      "CREATE TABLE type_integration_enum(i UInt8, x Enum('hello' = 1, 'world' = 2)) ENGINE Memory"
    )

    on_exit(fn -> Help.query!("DROP TABLE type_integration_enum") end)

    Ch.query!(
      pool,
      "INSERT INTO type_integration_enum VALUES (0, 'hello'), (1, 'world'), (2, 'hello')"
    )

    rowbinary =
      RowBinary.encode_rows(
        [[3, "hello"], [4, "world"], [5, 1], [6, 2]],
        ["UInt8", "Enum8('hello' = 1, 'world' = 2)"]
      )

    Ch.query!(pool, ["INSERT INTO type_integration_enum(i, x) FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT *, CAST(x, 'Int8') FROM type_integration_enum ORDER BY i").rows ==
             [
               [0, "hello", 1],
               [1, "world", 2],
               [2, "hello", 1],
               [3, "hello", 1],
               [4, "world", 2],
               [5, "hello", 1],
               [6, "world", 2]
             ]
  end

  test "map and tuple", %{pool: pool} do
    assert Ch.query!(pool, "SELECT {map:Map(String, UInt8)}, {tuple:Tuple(Int8, String)}", %{
             "map" => %{"pg" => 13, "hello" => 100},
             "tuple" => {-1, "abs"}
           }).rows == [[%{"hello" => 100, "pg" => 13}, {-1, "abs"}]]

    Help.query!("CREATE TABLE type_integration_tuple(a Tuple(String, Int64)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE type_integration_tuple") end)

    Ch.query!(pool, "INSERT INTO type_integration_tuple VALUES (('y', 10)), (('x', -10))")
    rowbinary = RowBinary.encode_rows([[{"a", 20}], [{"b", 30}]], ["Tuple(String, Int64)"])
    Ch.query!(pool, ["INSERT INTO type_integration_tuple FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT a FROM type_integration_tuple ORDER BY a.1").rows == [
             [{"a", 20}],
             [{"b", 30}],
             [{"x", -10}],
             [{"y", 10}]
           ]
  end

  test "datetime and datetime64 with timezone", %{pool: pool} do
    Help.query!("""
    CREATE TABLE type_integration_datetime(
      timestamp DateTime('Asia/Istanbul'),
      precise DateTime64(3, 'Asia/Istanbul'),
      event_id UInt8
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE type_integration_datetime") end)

    Ch.query!(pool, """
    INSERT INTO type_integration_datetime VALUES
    (1546300800, 1546300800123, 1),
    ('2019-01-01 00:00:00', '2019-01-01 00:00:00.123', 2)
    """)

    assert Ch.query!(
             pool,
             "SELECT *, toString(timestamp), toString(precise) FROM type_integration_datetime ORDER BY event_id"
           ).rows ==
             [
               [
                 DateTime.new!(~D[2019-01-01], ~T[03:00:00], "Asia/Istanbul"),
                 DateTime.new!(~D[2019-01-01], ~T[03:00:00.123], "Asia/Istanbul"),
                 1,
                 "2019-01-01 03:00:00",
                 "2019-01-01 03:00:00.123"
               ],
               [
                 DateTime.new!(~D[2019-01-01], ~T[00:00:00], "Asia/Istanbul"),
                 DateTime.new!(~D[2019-01-01], ~T[00:00:00.123], "Asia/Istanbul"),
                 2,
                 "2019-01-01 00:00:00",
                 "2019-01-01 00:00:00.123"
               ]
             ]
  end

  defp integer_param do
    one_of([
      typed_integer("Int8", -128..127),
      typed_integer("Int16", -32_768..32_767),
      typed_integer("Int32", -2_147_483_648..2_147_483_647),
      typed_integer("Int64", -9_007_199_254_740_992..9_007_199_254_740_991),
      typed_integer("UInt8", 0..255),
      typed_integer("UInt16", 0..65_535),
      typed_integer("UInt32", 0..4_294_967_295),
      typed_integer("UInt64", 0..9_007_199_254_740_991)
    ])
  end

  defp typed_integer(type, range) do
    gen all value <- integer(range) do
      {type, value}
    end
  end

  defp fixed_string_param do
    gen all size <- integer(1..12),
            value <- string(:alphanumeric, max_length: size) do
      {size, value}
    end
  end

  defp decimal_param do
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

  defp utc_datetime64 do
    gen all date <- date_gen(),
            hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      DateTime.new!(date, Time.new!(hour, minute, second, {microsecond, 6}), "Etc/UTC")
    end
  end

  defp tuple_param do
    gen all n <- integer(-128..127),
            string <- safe_string() do
      {n, string}
    end
  end

  defp bool_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 32),
            values <- list_of(boolean(), length: length(ids)) do
      Enum.zip_with(ids, values, fn id, value -> [id, value] end)
    end
  end

  defp date_gen do
    gen all days <- integer(0..20_000) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end
end
