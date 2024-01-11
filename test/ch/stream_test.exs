defmodule Ch.StreamTest do
  use ExUnit.Case

  setup do
    pool = start_supervised!({Ch, database: Ch.Test.database()})
    {:ok, pool: pool}
  end

  describe "enumerable" do
    test "works", %{pool: pool} do
      result =
        DBConnection.run(pool, fn conn ->
          conn
          |> Ch.stream("select * from system.numbers limit {limit:UInt32}", %{"limit" => 100})
          |> Enum.into([])
        end)

      assert [
               %Ch.Result{
                 command: :select,
                 data: [
                   <<1, 6, 110, 117, 109, 98, 101, 114, 6, 85, 73, 110, 116, 54, 52>>,
                   <<_::6400>>
                 ]
               },
               %Ch.Result{command: :select, data: []}
             ] = result
    end
  end

  describe "collectable" do
    test "works", %{pool: pool} do
      Ch.query!(pool, "create table collectable_test(a UInt64, b String) engine Null")

      rows =
        Stream.repeatedly(fn -> [0, "0"] end)
        |> Stream.chunk_every(10000)
        |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, ["UInt64", "String"]) end)
        |> Stream.take(3)

      result =
        DBConnection.run(pool, fn conn ->
          Enum.into(
            rows,
            Ch.stream(conn, "insert into collectable_test(a, b) format RowBinary\n")
          )
        end)

      assert result == :asdlkfhajsdf
    end
  end
end
