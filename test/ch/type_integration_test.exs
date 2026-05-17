defmodule Ch.TypeIntegrationTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "integer params round-trip", %{pool: pool} do
    assert Ch.query!(
             pool,
             """
             SELECT
               {i8:Int8},
               {i16:Int16},
               {i32:Int32},
               {i64:Int64},
               {u8:UInt8},
               {u16:UInt16},
               {u32:UInt32},
               {u64:UInt64}
             """,
             %{
               "i8" => -1,
               "i16" => -1000,
               "i32" => 100_000,
               "i64" => -1_000_000,
               "u8" => 1,
               "u16" => 1000,
               "u32" => 100_000,
               "u64" => 1_000_000
             }
           ).rows == [[-1, -1000, 100_000, -1_000_000, 1, 1000, 100_000, 1_000_000]]
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

    Help.query!("DROP TABLE IF EXISTS type_integration_fixed_string")
    Help.query!("CREATE TABLE type_integration_fixed_string(a FixedString(3)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_fixed_string") end)

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

    Help.query!("DROP TABLE IF EXISTS type_integration_decimal")
    Help.query!("CREATE TABLE type_integration_decimal(d Decimal32(4)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_decimal") end)

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
    Help.query!("DROP TABLE IF EXISTS type_integration_bool")
    Help.query!("CREATE TABLE type_integration_bool(a Int64, b Bool) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_bool") end)

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

  test "uuid", %{pool: pool} do
    uuid = "417ddc5d-e556-4d27-95dd-a34d84e46a50"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!(case: :lower)

    assert Ch.query!(pool, "SELECT {uuid:UUID}, toString({uuid:UUID})", %{"uuid" => uuid}).rows ==
             [[uuid_bin, uuid]]

    Help.query!("DROP TABLE IF EXISTS type_integration_uuid")
    Help.query!("CREATE TABLE type_integration_uuid(x UUID, y String) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_uuid") end)

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
    Help.query!("DROP TABLE IF EXISTS type_integration_enum")

    Help.query!(
      "CREATE TABLE type_integration_enum(i UInt8, x Enum('hello' = 1, 'world' = 2)) ENGINE Memory"
    )

    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_enum") end)

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

    Help.query!("DROP TABLE IF EXISTS type_integration_tuple")
    Help.query!("CREATE TABLE type_integration_tuple(a Tuple(String, Int64)) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_tuple") end)

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
    Help.query!("DROP TABLE IF EXISTS type_integration_datetime")

    Help.query!("""
    CREATE TABLE type_integration_datetime(
      timestamp DateTime('Asia/Istanbul'),
      precise DateTime64(3, 'Asia/Istanbul'),
      event_id UInt8
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE IF EXISTS type_integration_datetime") end)

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
end
