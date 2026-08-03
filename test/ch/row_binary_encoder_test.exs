defmodule Ch.RowBinaryEncoderTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  defmodule ListSchemaEncoder do
    require Ch.RowBinary.Encoder

    Ch.RowBinary.Encoder.define_encoder(
      name: :encode,
      schema: [z: "UInt8", a: "String", active: "Bool"],
      rows: true
    )
  end

  defmodule MapSchemaEncoder do
    require Ch.RowBinary.Encoder

    Ch.RowBinary.Encoder.define_encoder(
      name: :encode,
      schema: %{"z" => "UInt8", "a" => "String"},
      rows: true
    )
  end

  defmodule ListRowEncoder do
    require Ch.RowBinary.Encoder

    Ch.RowBinary.Encoder.define_encoder(
      name: :encode,
      schema: [id: "UInt64", text: "String", bytes: "Array(UInt8)"],
      row: :list,
      rows: true
    )
  end

  defmodule InsertEncoder do
    require Ch.RowBinary.Encoder

    Ch.RowBinary.Encoder.define_encoder(
      name: :insert,
      schema: [id: "UInt64", text: "String"],
      table: "analytics.events"
    )
  end

  defmodule MixedTypeEncoder do
    require Ch.RowBinary.Encoder

    Ch.RowBinary.Encoder.define_encoder(
      name: :encode,
      schema: [
        unsigned: "UInt16",
        signed: "Int8",
        ratio: "Float64",
        text: "String",
        fixed: "FixedString(4)",
        active: "Bool",
        bytes: "Array(UInt8)",
        optional: "Nullable(String)",
        date: "Date",
        date32: "Date32",
        time: "Time",
        datetime: "DateTime",
        datetime64: "DateTime64(3, 'UTC')",
        uuid: "UUID"
      ],
      row: :list
    )
  end

  describe "define_encoder/1" do
    test "list schemas preserve declared column order for map and struct rows" do
      rows = [%{z: 7, a: "value", active: true}]

      assert binary(ListSchemaEncoder.encode(rows)) ==
               binary(RowBinary.encode_rows([[7, "value", true]], ["UInt8", "String", "Bool"]))
    end

    test "map schemas sort columns by column name" do
      rows = [%{"z" => 7, "a" => "value"}]

      assert binary(MapSchemaEncoder.encode(rows)) ==
               binary(RowBinary.encode_rows([["value", 7]], ["String", "UInt8"]))
    end

    test "list rows use schema order and match the generic representation" do
      rows = [[1, "one", [1, 2, 3]], [2, "two", []]]
      types = ["UInt64", "String", "Array(UInt8)"]

      assert binary(ListRowEncoder.encode(rows)) == binary(RowBinary.encode_rows(rows, types))
    end

    test "specialized and fallback types match the generic codec" do
      types = [
        "UInt16",
        "Int8",
        "Float64",
        "String",
        "FixedString(4)",
        "Bool",
        "Array(UInt8)",
        "Nullable(String)",
        "Date",
        "Date32",
        "Time",
        "DateTime",
        "DateTime64(3, 'UTC')",
        "UUID"
      ]

      row = [
        65_535,
        -12,
        1.25,
        "hello",
        "ab",
        true,
        [1, 2, 3],
        "present",
        ~D[2026-08-02],
        ~D[1960-01-01],
        ~T[12:34:56],
        ~N[2026-08-02 12:34:56],
        ~U[2026-08-02 12:34:56.789Z],
        "d2bd5ec9-fdc5-a53f-32b5-e852f63a5f09"
      ]

      assert binary(MixedTypeEncoder.encode(row)) == binary(RowBinary.encode_row(row, types))
    end

    test "builds a complete insert body with a validated table identifier" do
      rows = [%{id: 1, text: "one"}, %{id: 2, text: "two"}]

      expected = [
        "INSERT INTO analytics.events FORMAT RowBinaryWithNamesAndTypes\n",
        RowBinary.encode_names_and_types(["id", "text"], ["UInt64", "String"]),
        RowBinary.encode_rows([[1, "one"], [2, "two"]], ["UInt64", "String"])
      ]

      assert binary(InsertEncoder.insert(rows)) == binary(expected)
    end

    test "rejects table strings that could alter the generated statement" do
      module = Module.concat(__MODULE__, :UnsafeTableEncoder)

      assert_raise ArgumentError, ~r/invalid ClickHouse table identifier/, fn ->
        Code.compile_quoted(
          quote do
            defmodule unquote(module) do
              require Ch.RowBinary.Encoder

              Ch.RowBinary.Encoder.define_encoder(
                name: :insert,
                schema: [id: "UInt8"],
                table: "events FORMAT CSV; DROP TABLE events"
              )
            end
          end
        )
      end
    end
  end

  describe "prepare/1" do
    test "reuses normalized types for encoding and decoding" do
      plan = RowBinary.prepare(["UInt64", "String", "Array(UInt8)"])
      rows = [[1, "one", [1, 2]], [2, "two", []]]

      encoded = RowBinary.encode_rows(rows, plan)

      assert binary(encoded) ==
               binary(RowBinary.encode_rows(rows, ["UInt64", "String", "Array(UInt8)"]))

      assert RowBinary.decode_rows(binary(encoded), plan) == rows
      assert RowBinary.encode_rows([], plan) == []
      assert RowBinary.decode_rows(<<>>, plan) == []
    end
  end

  defp binary(iodata), do: IO.iodata_to_binary(iodata)
end
