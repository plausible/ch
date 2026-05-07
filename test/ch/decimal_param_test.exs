defmodule Ch.DecimalParamTest do
  use ExUnit.Case,
    parameterize: [%{query_options: []}, %{query_options: [multipart: true]}],
    async: true

  use ExUnitProperties

  import Ch.Test, only: [parameterize_query: 4, parameterize_query!: 4]

  setup ctx do
    {:ok, conn} = Ch.start_link()
    {:ok, conn: conn, query_options: ctx[:query_options] || []}
  end

  test "decimal parameter boundaries", ctx do
    max_integer = String.duplicate("9", 76)
    max_scale = "0." <> String.duplicate("0", 75) <> "1"

    assert_decimal_param(ctx, Decimal.new("1.23"), "Decimal(76, 2)", Decimal.new("1.23"))
    assert_decimal_param(ctx, Decimal.new("-1.23"), "Decimal(76, 2)", Decimal.new("-1.23"))

    assert_decimal_param(
      ctx,
      Decimal.new(max_integer),
      "Decimal(76, 0)",
      Decimal.new(max_integer)
    )

    assert_decimal_param(
      ctx,
      Decimal.new(1, 1, -76),
      "Decimal(76, 76)",
      Decimal.new(max_scale)
    )
  end

  test "compact exponent Decimal params are not expanded before request", ctx do
    encoded = encoded_decimal_param(ctx.query_options, Decimal.new("1e1000000"))

    assert encoded =~ "1E+1000000"
    assert byte_size(encoded) < 300
  end

  test "decimal parameters reject over-limit values", ctx do
    assert decimal_error(ctx, Decimal.new(1, 1, 76), "Decimal(76, 0)") =~
             "Decimal value is too big: 1 digits were read: '1'e76. Expected to read decimal with scale 0 and precision 76: value 1E+76 cannot be parsed as Decimal(76, 0) for query parameter 'd'"

    assert decimal_error(ctx, Decimal.new(String.duplicate("9", 77)), "Decimal(76, 0)") =~
             "Too many digits (77 > 76) in decimal value: value 99999999999999999999999999999999999999999999999999999999999999999999999999999 cannot be parsed as Decimal(76, 0) for query parameter 'd'."

    assert decimal_error(ctx, Decimal.new("1e1000000"), "Decimal(76, 0)") =~
             "Decimal value is too big: 1 digits were read: '1'e1000000. Expected to read decimal with scale 0 and precision 76: value 1E+1000000 cannot be parsed as Decimal(76, 0) for query parameter 'd'."

    assert_raise ArgumentError, "ClickHouse Decimal values must be finite", fn ->
      decimal_error(ctx, Decimal.new("NaN"), "Decimal(76, 0)")
    end

    assert_raise ArgumentError, "ClickHouse Decimal values must be finite", fn ->
      decimal_error(ctx, Decimal.new("Infinity"), "Decimal(76, 0)")
    end
  end

  test "decimal parameters below declared scale round to zero", ctx do
    assert_decimal_param(
      ctx,
      Decimal.new(1, 1, -77),
      "Decimal(76, 76)",
      Decimal.new("0E-76")
    )
  end

  property "compact exponent Decimal integer params round-trip", ctx do
    check all(decimal <- compact_decimal_integer()) do
      assert_decimal_param(ctx, decimal, "Decimal(76, 0)", decimal)
    end
  end

  property "Decimal params with scale round-trip", ctx do
    check all(decimal <- decimal_with_scale()) do
      assert_decimal_param(ctx, decimal, "Decimal(76, 12)", decimal)
    end
  end

  defp assert_decimal_param(ctx, decimal, type, expected) do
    assert %Ch.Result{rows: [[actual, ^type]]} =
             parameterize_query!(
               ctx,
               "select {d:#{type}}, toTypeName({d:#{type}})",
               %{"d" => decimal},
               ctx.query_options
             )

    assert Decimal.compare(actual, expected) == :eq
  end

  defp decimal_error(ctx, decimal, type) do
    assert {:error, error} =
             parameterize_query(
               ctx,
               "select {d:#{type}}",
               %{"d" => decimal},
               ctx.query_options
             )

    Exception.message(error)
  end

  defp compact_decimal_integer do
    gen all(
          sign <- member_of([1, -1]),
          coef <- integer(1..9_999_999_999_999_999),
          exp <- integer(0..50)
        ) do
      Decimal.new(sign, coef, exp)
    end
  end

  defp decimal_with_scale do
    gen all(
          sign <- member_of([1, -1]),
          coef <- integer(1..9_999_999_999_999),
          exp <- integer(-12..20)
        ) do
      Decimal.new(sign, coef, exp)
    end
  end

  defp encoded_decimal_param(query_options, decimal) do
    query = Ch.Query.build("select {d:Decimal(76, 0)}", query_options)

    {query_params, _headers, body} =
      DBConnection.Query.encode(query, %{"d" => decimal}, [])

    case query_params do
      [{"param_d", value}] -> value
      [] -> IO.iodata_to_binary(body)
    end
  end
end
