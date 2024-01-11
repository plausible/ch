defmodule Ch.StreamTest do
  use ExUnit.Case
  alias Ch.{Result, RowBinary}

  setup do
    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  describe "Ch.stream/4" do
    test "emits %Ch.Result{}", %{conn: conn} do
      count = 1_000_000

      assert [%Result{command: :select, data: header} | rest] =
               DBConnection.run(conn, fn conn ->
                 conn
                 |> Ch.stream("select * from numbers({count:UInt64})", %{"count" => 1_000_000})
                 |> Enum.into([])
               end)

      assert header == [<<1, 6, "number", 6, "UInt64">>]

      decoded =
        Enum.flat_map(rest, fn %Result{data: data} ->
          data |> IO.iodata_to_binary() |> RowBinary.decode_rows([:u64])
        end)

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
end
