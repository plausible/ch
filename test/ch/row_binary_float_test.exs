defmodule Ch.RowBinaryFloatTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "float params round-trip through ClickHouse", %{pool: pool} do
    check all {type, value, expected} <- float_param() do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}, []).rows == [
               [expected]
             ]
    end
  end

  property "float arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all {type, values, expected} <- float_array_param() do
      assert [[actual]] =
               Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}, []).rows

      assert_float_array_equal(actual, expected)
    end
  end

  test "query params cover deterministic Float32 and Float64 cases", %{pool: pool} do
    cases = [
      {"Float32", 0, 0.0},
      {"Float32", -1.5, -1.5},
      {"Float32", 16_777_216, 16_777_216.0},
      {"Float32", -16_777_216, -16_777_216.0},
      {"Float64", 0, 0.0},
      {"Float64", -1.5, -1.5},
      {"Float64", 1.7976931348623157e308, 1.7976931348623157e308},
      {"Float64", 2.2250738585072014e-308, 2.2250738585072014e-308}
    ]

    for {type, value, expected} <- cases do
      assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}, []).rows == [
               [expected]
             ]
    end
  end

  test "query params cover nullable, empty array, and non-finite float behavior", %{pool: pool} do
    assert Ch.query!(
             pool,
             """
             SELECT
               {nullable32:Nullable(Float32)},
               {nullable64:Nullable(Float64)},
               {empty32:Array(Float32)},
               {empty64:Array(Float64)}
             """,
             %{
               "nullable32" => nil,
               "nullable64" => nil,
               "empty32" => [],
               "empty64" => []
             },
             []
           ).rows == [[nil, nil, [], []]]

    assert Ch.query!(
             pool,
             "SELECT {nan:Float64}, {inf:Float64}, {neg_inf:Float64}",
             %{"nan" => "NaN", "inf" => "inf", "neg_inf" => "-inf"},
             []
           ).rows == [[nil, nil, nil]]
  end

  test "query params reject invalid floats", %{pool: pool} do
    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:Float64}", %{"value" => "not-a-float"}, [])

    assert message =~ "Float64"
    assert message =~ "query parameter"

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:Float64}", %{"value" => nil}, [])

    assert message =~ "Float64"
    assert message =~ "query parameter"
  end

  property "RowBinary float inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_float_property (
      id UInt8,
      f32 Float32,
      f64 Float64
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_float_property") end)

    check all rows <- rowbinary_float_rows() do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_float_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "Float32", "Float64"])

      Ch.query!(pool, [
        "INSERT INTO row_binary_float_property FORMAT RowBinary\n" | rowbinary
      ])

      expected =
        rows
        |> Enum.map(fn [id, f32_value, f64_value] -> [id, f32(f32_value), f64_value] end)
        |> Enum.sort_by(&List.first/1)

      assert Ch.query!(pool, "SELECT * FROM row_binary_float_property ORDER BY id").rows ==
               expected
    end
  end

  test "RowBinary inserts cover scalar, nullable, array, tuple, point, and defaults", %{
    pool: pool
  } do
    Help.query!("""
    CREATE TABLE row_binary_float_representative (
      id UInt8,
      f32 Float32,
      f64 Float64,
      nullable32 Nullable(Float32),
      nullable64 Nullable(Float64),
      f32s Array(Float32),
      f64s Array(Float64),
      tuple Tuple(Float32, Float64),
      point Point
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_float_representative") end)

    rows = [
      [
        1,
        3.14159,
        2.718281828459045,
        nil,
        nil,
        [],
        [],
        {0.5, -0.25},
        {10.0, 20.5}
      ],
      [
        2,
        nil,
        nil,
        -1.5,
        42.25,
        [-1.5, 0, 3.25],
        [-1.5, 0, 3.25],
        {3.14159, 2.718281828459045},
        {-5.5, 4.25}
      ],
      [
        3,
        3.4028234663852886e38,
        1.7976931348623157e308,
        1.1754943508222875e-38,
        2.2250738585072014e-308,
        [1.1754943508222875e-38, 3.4028234663852886e38],
        [2.2250738585072014e-308, 1.7976931348623157e308],
        {-16_777_216, 16_777_216},
        {0.0, -0.0}
      ]
    ]

    types = [
      "UInt8",
      "Float32",
      "Float64",
      "Nullable(Float32)",
      "Nullable(Float64)",
      "Array(Float32)",
      "Array(Float64)",
      "Tuple(Float32, Float64)",
      "Point"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, ["INSERT INTO row_binary_float_representative FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_float_representative ORDER BY id").rows == [
             [
               1,
               f32(3.14159),
               2.718281828459045,
               nil,
               nil,
               [],
               [],
               {0.5, -0.25},
               {10.0, 20.5}
             ],
             [
               2,
               0.0,
               0.0,
               -1.5,
               42.25,
               [-1.5, 0.0, 3.25],
               [-1.5, 0.0, 3.25],
               {f32(3.14159), 2.718281828459045},
               {-5.5, 4.25}
             ],
             [
               3,
               f32(3.4028234663852886e38),
               1.7976931348623157e308,
               f32(1.1754943508222875e-38),
               2.2250738585072014e-308,
               [f32(1.1754943508222875e-38), f32(3.4028234663852886e38)],
               [2.2250738585072014e-308, 1.7976931348623157e308],
               {-16_777_216.0, 16_777_216.0},
               {0.0, -0.0}
             ]
           ]
  end

  test "RowBinary rejects invalid float values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode(:f32, "1.0")
    end

    assert_raise FunctionClauseError, fn ->
      RowBinary.encode(:f64, "1.0")
    end

    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["1.0"]], ["Float64"])
    end
  end

  defp float_param do
    one_of([
      gen all value <- finite_float32_param() do
        {"Float32", value, f32(value)}
      end,
      gen all value <- finite_float() do
        {"Float64", value, value}
      end
    ])
  end

  defp float_array_param do
    one_of([
      gen all values <- list_of(finite_float32_param(), max_length: 8) do
        {"Float32", values, Enum.map(values, &f32/1)}
      end,
      gen all values <- list_of(finite_float(), max_length: 8) do
        {"Float64", values, values}
      end
    ])
  end

  defp rowbinary_float_rows do
    uniq_list_of(
      fixed_list([
        integer(0..255),
        finite_float(),
        finite_float()
      ]),
      max_length: 12
    )
  end

  defp finite_float do
    gen all coefficient <- integer(-1_000_000_000..1_000_000_000),
            divisor <- member_of([1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]) do
      coefficient / divisor
    end
  end

  defp finite_float32_param do
    gen all coefficient <- integer(-10_000..10_000),
            divisor <- member_of([1, 2, 4, 8, 16, 32, 64, 128, 256]) do
      coefficient / divisor
    end
  end

  defp f32(value) do
    <<rounded::32-little-float>> = <<value::32-little-float>>
    rounded
  end

  defp assert_float_array_equal(actual, expected) do
    assert length(actual) == length(expected)

    Enum.zip(actual, expected)
    |> Enum.each(fn {actual_value, expected_value} ->
      assert_in_delta actual_value, expected_value, float_delta(expected_value)
    end)
  end

  defp float_delta(value), do: max(abs(value) * 1.0e-12, 1.0e-12)
end
