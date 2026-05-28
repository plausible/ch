defmodule Ch.RowBinaryGeoTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "Point params round-trip through ClickHouse", %{pool: pool} do
    check all point <- point_gen() do
      assert [[decoded]] = Ch.query!(pool, "SELECT {value:Point}", %{"value" => point}).rows
      assert_in_delta elem(decoded, 0), elem(point, 0), 1.0e-9
      assert_in_delta elem(decoded, 1), elem(point, 1), 1.0e-9
    end
  end

  property "RowBinary Point inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("CREATE TABLE row_binary_geo_point_property(id UInt64, p Point) ENGINE Memory")
    on_exit(fn -> Help.query!("DROP TABLE row_binary_geo_point_property") end)

    check all rows <- point_rows(), max_runs: 25 do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_geo_point_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt64", "Point"])

      Ch.query!(pool, ["INSERT INTO row_binary_geo_point_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_geo_point_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary inserts cover Ring, Polygon, and MultiPolygon aliases", %{pool: pool} do
    Help.query!(
      "CREATE TABLE row_binary_geo_values(id UInt64, r Ring, p Polygon, mp MultiPolygon) ENGINE Memory"
    )

    on_exit(fn -> Help.query!("DROP TABLE row_binary_geo_values") end)

    ring = [{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}]
    polygon = [ring, [{2.0, 2.0}, {4.0, 2.0}, {4.0, 4.0}]]
    multipolygon = [polygon, [[{20.0, 20.0}, {30.0, 20.0}, {30.0, 30.0}]]]

    rows = [
      [0, [], [], []],
      [1, ring, polygon, multipolygon]
    ]

    rowbinary = RowBinary.encode_rows(rows, ["UInt64", "Ring", "Polygon", "MultiPolygon"])
    Ch.query!(pool, ["INSERT INTO row_binary_geo_values FORMAT RowBinary\n" | rowbinary])

    assert Ch.query!(pool, "SELECT * FROM row_binary_geo_values ORDER BY id").rows == rows
  end

  test "RowBinary rejects invalid Point values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["not a point"]], ["Point"])
    end
  end

  defp point_rows do
    gen all ids <- uniq_list_of(integer(0..18_446_744_073_709_551_615), max_length: 16),
            points <- list_of(point_gen(), length: length(ids)) do
      Enum.zip_with(ids, points, fn id, point -> [id, point] end)
    end
  end

  defp point_gen do
    gen all x <- integer(-1_000_000..1_000_000),
            y <- integer(-1_000_000..1_000_000) do
      {x * 1.0, y * 1.0}
    end
  end
end
