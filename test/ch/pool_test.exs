defmodule Ch.PoolTest do
  use ExUnit.Case, async: true

  setup ctx do
    Help.setup_pool(ctx)
  end

  test "select", ctx do
    assert Help.query!(ctx, "select 1").rows == [[1]]
    assert Help.query!(ctx, "select {a:UInt8}", a: 2).rows == [[2]]

    uuid = "9B29BD20-924C-4DE5-BDB3-8C2AA1FCE1FC"
    uuid_bin = uuid |> String.replace("-", "") |> Base.decode16!()

    params = [
      {"UInt8", 1},
      {"Bool", true},
      {"Bool", false},
      {"Nullable(Nothing)", nil},
      {"Float32", 1.0},
      {"Float64", 1.0},
      {"String", "a&b=c"},
      {"String", "a\n"},
      {"String", "a\t"},
      {"Array(String)", ["a\tb"]},
      {"Array(Bool)", [true, false]},
      {"Array(Nullable(String))", ["a", nil, "b"]},
      {"Decimal(9,4)", Decimal.new("2000.3330")},
      {"Decimal(9,4)", Decimal.new("2000.333"), Decimal.new("2000.3330")},
      {"Date", ~D[2022-01-01]},
      {"Array(Date)", [~D[2022-01-01], ~D[2022-01-02]]},
      {"Date32", ~D[2022-01-01]},
      {"Array(String)", ["a", "b'", "\\'c"]},
      {"Array(String)", ["a\n", "b\tc"]},
      {"Array(UInt8)", [1, 2, 3]},
      {"Array(Array(UInt8))", [[1], [2, 3], []]},
      {"UUID", uuid, uuid_bin}
    ]

    Enum.each(params, fn param ->
      {type, value, expected} =
        case param do
          {type, value} -> {type, value, value}
          {_type, _value, _expected} -> param
        end

      assert Help.query!(ctx, "select {a:#{type}}", %{"a" => value}).rows == [[expected]]
    end)
  end

  test "insert", ctx do
    assert Help.query!(ctx, "create temporary table test_insert(a UInt8, b String) engine Memory")

    # params
    assert Help.query!(
             ctx,
             "insert into test_insert values (1, 'hello'), ({two:UInt8}, {world:String})",
             %{"two" => "2", "world" => "world"}
           )

    types = ["UInt8", "String"]

    # rowbinary
    assert Help.query!(ctx, [
             "insert into test_insert format RowBinaryWithNamesAndTypes\n",
             Ch.RowBinary.encode_names_and_types(["a", "b"], types)
             | Ch.RowBinary.encode_rows([[3, "foo"], [4, "bar"], [5, "baz"]], types)
           ])

    # compressed rowbinary
    assert Help.query!(
             ctx,
             :zstd.compress([
               "insert into test_insert format RowBinaryWithNamesAndTypes\n",
               Ch.RowBinary.encode_names_and_types(["a", "b"], types)
               | Ch.RowBinary.encode_rows(
                   [[6, "clickhouse"], [7, "postgres"], [8, "sqlite"]],
                   types
                 )
             ]),
             _params = %{},
             headers: [{"content-encoding", "zstd"}]
           )

    assert Help.query!(ctx, "select * from test_insert order by a asc").rows == [
             [1, "hello"],
             [2, "world"],
             [3, "foo"],
             [4, "bar"],
             [5, "baz"],
             [6, "clickhouse"],
             [7, "postgres"],
             [8, "sqlite"]
           ]
  end

  test "checkout", ctx do
    assert Ch.Pool.checkout(ctx.pool, fn conn ->
             assert Ch.Pool.query!(conn, "select 1").rows == [[1]]
             Ch.Pool.query!(conn, "select {a:UInt8}", a: 2)
           end).rows == [[2]]
  end

  test "decode false returns the raw response body", ctx do
    assert is_binary(Help.query!(ctx, "select 1", %{}, decode: false))
  end
end
