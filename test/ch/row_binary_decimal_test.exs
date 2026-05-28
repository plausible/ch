defmodule Ch.RowBinaryDecimalTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "decimal params round-trip through ClickHouse across storage widths", %{pool: pool} do
    check all {type, value, expected} <- decimal_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[expected]]
    end
  end

  property "decimal arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all {type, values, expected} <- decimal_array_param() do
      assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
               [expected]
             ]
    end
  end

  test "query params cover deterministic decimal precisions", %{pool: pool} do
    cases = [
      {"Decimal32(4)", Decimal.new("0"), Decimal.new("0.0000")},
      {"Decimal32(4)", Decimal.new("12345.6789"), Decimal.new("12345.6789")},
      {"Decimal64(6)", Decimal.new("-123456789.123456"), Decimal.new("-123456789.123456")},
      {"Decimal128(9)", Decimal.new("123456789012345678.123456789"),
       Decimal.new("123456789012345678.123456789")},
      {"Decimal256(18)",
       Decimal.new(-1, 123_456_789_012_345_678_901_234_567_890_123_456_789, -18),
       Decimal.new(-1, 123_456_789_012_345_678_901_234_567_890_123_456_789, -18)}
    ]

    for {type, value, expected} <- cases do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[expected]]
    end
  end

  test "nullable, empty array, and invalid decimal params through ClickHouse", %{pool: pool} do
    assert Ch.query!(
             pool,
             "SELECT {nullable:Nullable(Decimal(18, 4))}, {empty:Array(Decimal(18, 4))}",
             %{"nullable" => nil, "empty" => []}
           ).rows == [[nil, []]]

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:Decimal(18, 4)}", %{"value" => "not-a-decimal"})

    assert message =~ "Decimal"
  end

  property "RowBinary decimal inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_decimal_property (
      id UInt8,
      d32 Decimal32(4),
      d64 Decimal64(6),
      d128 Decimal128(9),
      d256 Decimal256(18)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_decimal_property") end)

    check all rows <- rowbinary_decimal_rows(), max_runs: 20 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_decimal_property")

      rowbinary =
        RowBinary.encode_rows(
          rows,
          ["UInt8", "Decimal32(4)", "Decimal64(6)", "Decimal128(9)", "Decimal256(18)"]
        )

      Ch.query!(pool, ["INSERT INTO row_binary_decimal_property FORMAT RowBinary\n" | rowbinary])

      expected = Enum.sort_by(rows, &List.first/1)

      assert Ch.query!(pool, "SELECT * FROM row_binary_decimal_property ORDER BY id").rows ==
               expected
    end
  end

  test "RowBinary inserts cover nullable, arrays, tuples, and defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_decimal_representative (
      id UInt8,
      d Decimal(18, 4),
      nullable Nullable(Decimal(18, 4)),
      decimals Array(Decimal(18, 4)),
      pair Tuple(Decimal(9, 2), Decimal(18, 4))
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_decimal_representative") end)

    rows = [
      [1, Decimal.new("1.23"), nil, [], {Decimal.new("1.2"), Decimal.new("-2.3456")}],
      [
        2,
        nil,
        Decimal.new("-4.56789"),
        [Decimal.new("0"), Decimal.new("123.45678")],
        {Decimal.new("99.999"), Decimal.new("42")}
      ]
    ]

    types = [
      "UInt8",
      "Decimal(18, 4)",
      "Nullable(Decimal(18, 4))",
      "Array(Decimal(18, 4))",
      "Tuple(Decimal(9, 2), Decimal(18, 4))"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, [
      "INSERT INTO row_binary_decimal_representative FORMAT RowBinary\n" | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM row_binary_decimal_representative ORDER BY id").rows ==
             [
               [
                 1,
                 Decimal.new("1.2300"),
                 nil,
                 [],
                 {Decimal.new("1.20"), Decimal.new("-2.3456")}
               ],
               [
                 2,
                 Decimal.new("0.0000"),
                 Decimal.new("-4.5679"),
                 [Decimal.new("0.0000"), Decimal.new("123.4568")],
                 {Decimal.new("100.00"), Decimal.new("42.0000")}
               ]
             ]
  end

  test "RowBinary rejects invalid decimal values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["1.23"]], ["Decimal(18, 4)"])
    end
  end

  defp decimal_param do
    one_of([
      typed_decimal("Decimal32(4)", decimal_gen(5, 4)),
      typed_decimal("Decimal64(6)", decimal_gen(12, 6)),
      typed_decimal("Decimal128(9)", decimal_gen(20, 9)),
      typed_decimal("Decimal256(18)", decimal_gen(30, 18))
    ])
  end

  defp decimal_array_param do
    one_of([
      typed_decimal_array("Decimal32(4)", decimal_gen(5, 4)),
      typed_decimal_array("Decimal64(6)", decimal_gen(12, 6)),
      typed_decimal_array("Decimal128(9)", decimal_gen(20, 9)),
      typed_decimal_array("Decimal256(18)", decimal_gen(30, 18))
    ])
  end

  defp typed_decimal(type, generator) do
    gen all value <- generator do
      {type, value, value}
    end
  end

  defp typed_decimal_array(type, generator) do
    gen all values <- list_of(generator, max_length: 4) do
      {type, values, values}
    end
  end

  defp rowbinary_decimal_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 12),
            values <-
              list_of(
                fixed_list([
                  decimal_gen(5, 4),
                  decimal_gen(12, 6),
                  decimal_gen(20, 9),
                  decimal_gen(30, 18)
                ]),
                length: length(ids)
              ) do
      Enum.zip_with(ids, values, fn id, decimals -> [id | decimals] end)
    end
  end

  defp decimal_gen(integer_digits, scale) do
    gen all sign <- member_of([1, -1]),
            coefficient <- integer(0..(Integer.pow(10, integer_digits + scale) - 1)) do
      Decimal.new(sign, coefficient, -scale)
    end
  end
end
