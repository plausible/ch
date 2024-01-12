defmodule Ch.StreamTest do
  use ExUnit.Case
  alias Ch.{Result, RowBinary}

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  describe "enumerable Ch.stream/4" do
    test "emits %Ch.Result{}", %{conn: conn} do
      count = 1_000_000

      assert [%Result{command: :select, data: header} | _rest] =
               results =
               DBConnection.run(conn, fn conn ->
                 conn
                 |> Ch.stream("select * from numbers({count:UInt64})", %{"count" => 1_000_000})
                 |> Enum.into([])
               end)

      assert [<<1, 6, "number", 6, "UInt64">> | _] = header

      decoded = results |> Enum.map(& &1.data) |> IO.iodata_to_binary() |> RowBinary.decode_rows()

      assert [[0], [1], [2] | _] = decoded
      assert length(decoded) == count
    end

    test "raises on error", %{conn: conn} do
      assert_raise Ch.Error,
                   ~r/Code: 62. DB::Exception: Syntax error: failed at position 8/,
                   fn ->
                     DBConnection.run(conn, fn conn ->
                       conn |> Ch.stream("select ", %{"count" => 1_000_000}) |> Enum.into([])
                     end)
                   end
    end
  end

  describe "collectable Ch.stream/4" do
    test "inserts chunks", %{conn: conn} do
      Ch.query!(conn, "create table collect_stream(i UInt64) engine Memory")

      assert %Ch.Result{command: :insert, num_rows: 1_000_000} =
               DBConnection.run(conn, fn conn ->
                 Stream.repeatedly(fn -> [:rand.uniform(100)] end)
                 |> Stream.chunk_every(100_000)
                 |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, _types = ["UInt64"]) end)
                 |> Stream.take(10)
                 |> Enum.into(Ch.stream(conn, "insert into collect_stream(i) format RowBinary\n"))
               end)

      assert Ch.query!(conn, "select count(*) from collect_stream").rows == [[1_000_000]]
    end
  end
end
