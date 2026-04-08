defmodule Ch.PoolTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!({Ch.Pool, scheme: :http, host: "localhost", port: 8123})}
  end

  test "select", %{pool: pool} do
    assert Ch.Pool.query!(pool, "select 1").rows == [[1]]

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

      assert Ch.Pool.query!(pool, "select {a:#{type}}", %{"a" => value}).rows == [[expected]]
    end)
  end

  test "insert", %{pool: pool} do
    settings = [session_id: "test_insert_#{System.unique_integer()}"]

    session_query = fn statement ->
      Ch.Pool.query!(pool, statement, %{}, settings: settings)
    end

    assert session_query.("create temporary table test_insert(a UInt8, b String) engine Memory")

    assert session_query.("insert into test_insert values (1, 'hello')")

    types = ["UInt8", "String"]

    rowbinary = [
      Ch.RowBinary.encode_names_and_types(["a", "b"], types)
      | Ch.RowBinary.encode_rows([[2, "world"], [3, "foo"], [4, "bar"]], types)
    ]

    assert session_query.([
             "insert into test_insert format RowBinaryWithNamesAndTypes\n" | rowbinary
           ])

    assert session_query.("select * from test_insert order by a asc").rows == [
             [1, "hello"],
             [2, "world"],
             [3, "foo"],
             [4, "bar"]
           ]
  end
end
