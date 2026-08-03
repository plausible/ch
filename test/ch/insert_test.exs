defmodule Ch.InsertTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "inserts heterogeneous rows as one RowBinary stream", %{pool: pool} do
    Help.query!("""
    CREATE TABLE insert_matrix (
      id UInt8,
      signed Int64,
      unsigned UInt64,
      float64 Float64,
      decimal Decimal(18, 4),
      active Bool,
      string String,
      fixed FixedString(4),
      nullable Nullable(String),
      uuid UUID,
      date Date,
      datetime DateTime64(6, 'UTC'),
      ints Array(Int16),
      tuple Tuple(String, Int8),
      map Map(String, UInt8)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE insert_matrix") end)

    uuid = Base.decode16!("417DDC5DE5564D2795DDA34D84E46A50")
    zero_uuid = <<0::128>>

    rows = [
      [
        1,
        -42,
        42,
        1.5,
        Decimal.new("12.3400"),
        true,
        "line\nwith\ttabs",
        "AB",
        nil,
        uuid,
        ~D[2024-02-29],
        ~U[2024-02-29 12:34:56.123456Z],
        [-2, -1, 0, 1, 2],
        {"tuple", -8},
        %{"a" => 1, "b" => 2}
      ],
      [
        2,
        0,
        0,
        0.0,
        Decimal.new("0.0000"),
        false,
        "",
        "",
        "",
        zero_uuid,
        ~D[1970-01-01],
        ~U[1970-01-01 00:00:00.000000Z],
        [],
        {"", 0},
        %{}
      ],
      [
        3,
        -9_223_372_036_854_775_808,
        18_446_744_073_709_551_615,
        1.7976931348623157e308,
        Decimal.new("99999999999999.9999"),
        true,
        <<0, 255>>,
        "WXYZ",
        nil,
        uuid,
        ~D[2100-01-01],
        ~U[2100-01-01 23:59:59.999999Z],
        [-32_768, 32_767],
        {"edge", 127},
        %{"max" => 255}
      ]
    ]

    types = [
      "UInt8",
      "Int64",
      "UInt64",
      "Float64",
      "Decimal(18, 4)",
      "Bool",
      "String",
      "FixedString(4)",
      "Nullable(String)",
      "UUID",
      "Date",
      "DateTime64(6, 'UTC')",
      "Array(Int16)",
      "Tuple(String, Int8)",
      "Map(String, UInt8)"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)
    Ch.query!(pool, ["INSERT INTO insert_matrix FORMAT RowBinary\n" | rowbinary])

    expected =
      rows
      |> Enum.map(fn row -> List.update_at(row, 7, &String.pad_trailing(&1, 4, <<0>>)) end)
      |> Enum.sort_by(&List.first/1)

    assert Ch.query!(pool, "SELECT * FROM insert_matrix ORDER BY id").rows == expected
  end
end
