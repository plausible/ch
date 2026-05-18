defmodule Ch.SelectTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "selects variable numbers of params and columns", %{pool: pool} do
    for count <- [1, 2, 8, 32] do
      select =
        Enum.map_join(1..count, ", ", fn i ->
          "{p#{i}:UInt64} AS col_#{i}"
        end)

      params =
        1..count
        |> Map.new(fn i -> {"p#{i}", i * 10} end)

      assert %{names: names, rows: [row], data: data} =
               Ch.query!(pool, "SELECT #{select}", params)

      assert names == Enum.map(1..count, &"col_#{&1}")
      assert row == Enum.map(1..count, &(&1 * 10))

      assert [^names, ^row] =
               data
               |> IO.iodata_to_binary()
               |> Ch.RowBinary.decode_names_and_rows()
    end
  end

  test "decodes edge case selected values", %{pool: pool} do
    assert %{names: names, rows: [row], data: data} =
             Ch.query!(
               pool,
               """
               SELECT
                 {empty:String} AS empty_string,
                 {special:String} AS special_string,
                 {nil:Nullable(String)} AS nil_string,
                 {ints:Array(Int16)} AS ints,
                 {map:Map(String, UInt8)} AS map,
                 {tuple:Tuple(Int8, String)} AS tuple
               """,
               %{
                 "empty" => "",
                 "special" => "line\n tab\t ampersand& equals= quote'",
                 "nil" => nil,
                 "ints" => [-2, -1, 0, 1, 2],
                 "map" => %{"a" => 1, "b" => 2},
                 "tuple" => {-8, "tuple-value"}
               }
             )

    assert names == [
             "empty_string",
             "special_string",
             "nil_string",
             "ints",
             "map",
             "tuple"
           ]

    assert row == [
             "",
             "line\n tab\t ampersand& equals= quote'",
             nil,
             [-2, -1, 0, 1, 2],
             %{"a" => 1, "b" => 2},
             {-8, "tuple-value"}
           ]

    assert [^names, ^row] =
             data
             |> IO.iodata_to_binary()
             |> Ch.RowBinary.decode_names_and_rows()
  end

  test "decodes column names and types when select returns no rows", %{pool: pool} do
    assert %{names: names, rows: [], data: data} =
             Ch.query!(
               pool,
               """
               SELECT
                 {id:UInt64} AS id,
                 {name:String} AS name,
                 {nullable:Nullable(UInt8)} AS nullable
               WHERE 0
               """,
               %{"id" => 1, "name" => "name", "nullable" => nil}
             )

    assert names == ["id", "name", "nullable"]
    assert [^names] = data |> IO.iodata_to_binary() |> Ch.RowBinary.decode_names_and_rows()
  end

  test "selects a very large number of decoded columns", %{pool: pool} do
    column_count = 5_000
    select = Enum.map_join(1..column_count, ", ", fn i -> "#{i} AS col_#{i}" end)

    assert %{names: columns, rows: [row], data: data} = Ch.query!(pool, "SELECT #{select}")

    assert length(columns) == column_count
    assert length(row) == column_count
    assert Enum.take(columns, 3) == ["col_1", "col_2", "col_3"]
    assert Enum.take(row, 3) == [1, 2, 3]
    assert Enum.take(columns, -3) == ["col_4998", "col_4999", "col_5000"]
    assert Enum.take(row, -3) == [4998, 4999, 5000]

    assert [^columns, ^row] =
             data
             |> IO.iodata_to_binary()
             |> Ch.RowBinary.decode_names_and_rows()
  end
end
