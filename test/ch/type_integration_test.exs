defmodule Ch.TypeIntegrationTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "UUID params accept canonical text and decode to 16 bytes", %{pool: pool} do
    check all {uuid_text, uuid_bin} <- uuid_param() do
      assert Ch.query!(pool, "SELECT {value:UUID}, toString({value:UUID})", %{
               "value" => uuid_text
             }).rows == [[uuid_bin, String.downcase(uuid_text)]]
    end
  end

  test "booleans preserve ClickHouse coercion semantics", %{pool: pool} do
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

  test "UUID defaults and RowBinary inserts", %{pool: pool} do
    uuid = "417ddc5d-e556-4d27-95dd-a34d84e46a50"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!(case: :lower)

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

  test "Enum8 accepts labels and numeric representations", %{pool: pool} do
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

  test "Enum labels with quotes and backslashes round-trip through type headers", %{pool: pool} do
    type =
      {:enum8,
       [
         {"can't", 1},
         {"back\\slash", 2},
         {"comma, equals= parens()", 3},
         {"line\nbreak", 4},
         {"null\0byte", 5}
       ]}

    encoded_type = type |> Ch.Types.encode() |> IO.iodata_to_binary()

    Help.query!([
      "CREATE TABLE type_integration_enum_escaping(x ",
      encoded_type,
      ") ENGINE Memory"
    ])

    on_exit(fn -> Help.query!("DROP TABLE type_integration_enum_escaping") end)

    rows = [
      ["can't"],
      ["back\\slash"],
      ["comma, equals= parens()"],
      ["line\nbreak"],
      ["null\0byte"]
    ]

    rowbinary = RowBinary.encode_rows(rows, [type])
    Ch.query!(pool, ["INSERT INTO type_integration_enum_escaping FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(
             pool,
             "SELECT x FROM type_integration_enum_escaping ORDER BY CAST(x, 'Int8')"
           ).rows ==
             rows
  end

  test "DateTime and DateTime64 preserve declared timezones", %{pool: pool} do
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

  defp bool_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 32),
            values <- list_of(boolean(), length: length(ids)) do
      Enum.zip_with(ids, values, fn id, value -> [id, value] end)
    end
  end
end
