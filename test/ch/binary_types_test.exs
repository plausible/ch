defmodule Ch.BinaryTypesTest do
  use ExUnit.Case, async: true

  # https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding

  setup do
    {:ok, conn: start_supervised!(Ch)}
  end

  test "it works", %{conn: conn} do
    # Rexbug.start("Ch.RowBinary :: return;stack", msgs: 10000)
    # on_exit(fn -> :timer.sleep(100) end)

    %Ch.Result{columns: columns, rows: [row]} =
      Ch.query!(
        conn,
        """
        select
          toUInt8(1) as UInt8,
          toUInt16(1) as UInt16
        """,
        [],
        settings: [output_format_binary_encode_types_in_binary_format: 1]
      )

    assert Enum.zip(columns, row) == [{"UInt8", 1}, {"UInt16", 1}]
  end
end
