defmodule Ch.RowBinaryUUIDTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "UUID params accept canonical text and decode to bytes", %{pool: pool} do
    check all {text, bytes} <- uuid_param() do
      assert Ch.query!(pool, "SELECT {value:UUID}, toString({value:UUID})", %{
               "value" => text
             }).rows == [[bytes, text]]
    end
  end

  property "UUID arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all values <- list_of(uuid_param(), max_length: 8) do
      texts = Enum.map(values, fn {text, _bytes} -> text end)
      bytes = Enum.map(values, fn {_text, bytes} -> bytes end)

      assert Ch.query!(pool, "SELECT {value:Array(UUID)}", %{"value" => texts}).rows == [
               [bytes]
             ]
    end
  end

  test "query params cover uppercase text, nil, empty arrays, and invalid UUIDs", %{pool: pool} do
    uuid = "417ddc5d-e556-4d27-95dd-a34d84e46a50"
    uppercase = String.upcase(uuid)
    bytes = uuid_to_binary(uuid)

    assert Ch.query!(
             pool,
             "SELECT {uuid:UUID}, toString({uuid:UUID}), {nullable:Nullable(UUID)}, {empty:Array(UUID)}",
             %{"uuid" => uppercase, "nullable" => nil, "empty" => []}
           ).rows == [[bytes, uuid, nil, []]]

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:UUID}", %{"value" => "not-a-uuid"})

    assert message =~ "UUID"
  end

  property "RowBinary UUID inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_uuid_property (
      id UInt8,
      value UUID
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_uuid_property") end)

    check all rows <- rowbinary_uuid_rows() do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_uuid_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "UUID"])
      Ch.query!(pool, ["INSERT INTO row_binary_uuid_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_uuid_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary inserts cover nullable, arrays, tuples, and defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_uuid_representative (
      id UInt8,
      value UUID,
      nullable Nullable(UUID),
      uuids Array(UUID),
      pair Tuple(UUID, UUID)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_uuid_representative") end)

    uuid1 = uuid_to_binary("417ddc5d-e556-4d27-95dd-a34d84e46a50")
    uuid2 = uuid_to_binary("00010203-0405-0607-0809-0a0b0c0d0e0f")

    rows = [
      [1, uuid1, nil, [], {uuid1, uuid2}],
      [2, nil, uuid2, [uuid1, uuid2], {uuid2, uuid1}]
    ]

    types = ["UInt8", "UUID", "Nullable(UUID)", "Array(UUID)", "Tuple(UUID, UUID)"]
    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO row_binary_uuid_representative FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_uuid_representative ORDER BY id").rows == [
             [1, uuid1, nil, [], {uuid1, uuid2}],
             [2, <<0::128>>, uuid2, [uuid1, uuid2], {uuid2, uuid1}]
           ]
  end

  test "RowBinary rejects invalid UUID values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["not-a-uuid"]], ["UUID"])
    end
  end

  defp rowbinary_uuid_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 16),
            values <- list_of(uuid_bytes(), length: length(ids)) do
      Enum.zip_with(ids, values, fn id, value -> [id, value] end)
    end
  end

  defp uuid_param do
    gen all bytes <- uuid_bytes() do
      <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2),
        e::binary-size(6)>> = bytes

      text =
        [a, b, c, d, e]
        |> Enum.map_join("-", &Base.encode16(&1, case: :lower))

      {text, bytes}
    end
  end

  defp uuid_bytes do
    binary(length: 16)
  end

  defp uuid_to_binary(uuid) do
    uuid
    |> String.replace("-", "")
    |> Base.decode16!(case: :lower)
  end
end
