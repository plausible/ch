defmodule Ch.StreamTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]
  alias Ch.{Result, RowBinary}

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  describe "enumerable Ch.stream/4" do
    test "emits %Ch.Result{}", %{conn: conn, query_options: query_options} do
      results =
        DBConnection.run(conn, fn conn ->
          conn
          |> Ch.stream(
            "select * from numbers({count:UInt64})",
            %{"count" => 1_000_000},
            query_options
          )
          |> Enum.into([])
        end)

      assert results |> Enum.map(fn %Result{rows: rows} -> rows end) |> List.flatten() ==
               Enum.to_list(0..999_999)
    end

    test "raises on error", %{conn: conn, query_options: query_options} do
      assert_raise Ch.Error,
                   ~r/Code: 62. DB::Exception: Syntax error: failed at position 8/,
                   fn ->
                     DBConnection.run(conn, fn conn ->
                       conn
                       |> Ch.stream("select ", %{"count" => 1_000_000}, query_options)
                       |> Enum.into([])
                     end)
                   end
    end

    test "large strings", %{conn: conn, query_options: query_options} do
      results =
        DBConnection.run(conn, fn conn ->
          conn
          |> Ch.stream(
            "select repeat('abc', 500000) from numbers({count:UInt64})",
            %{"count" => 10},
            query_options
          )
          |> Enum.into([])
        end)

      expected_string = String.duplicate("abc", 500_000)

      assert results |> Enum.map(fn %Result{rows: rows} -> rows end) |> List.flatten() ==
               List.duplicate(expected_string, 10)
    end
  end

  describe "collectable Ch.stream/4" do
    test "inserts chunks", %{conn: conn, query_options: query_options} do
      Ch.query!(conn, "create table collect_stream(i UInt64) engine Memory")
      on_exit(fn -> Ch.Test.query("DROP TABLE collect_stream") end)

      assert %Ch.Result{command: :insert, num_rows: 1_000_000} =
               DBConnection.run(conn, fn conn ->
                 Stream.repeatedly(fn -> [:rand.uniform(100)] end)
                 |> Stream.chunk_every(100_000)
                 |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, _types = ["UInt64"]) end)
                 |> Stream.take(10)
                 |> Enum.into(
                   Ch.stream(
                     conn,
                     "insert into collect_stream(i) format RowBinary",
                     _params = [],
                     Keyword.merge(query_options, encode: false)
                   )
                 )
               end)

      assert Ch.query!(conn, "select count(*) from collect_stream").rows == [[1_000_000]]
    end
  end
end
