defmodule Ch.SettingsTest do
  use ExUnit.Case

  test "can start without settings" do
    assert {:ok, conn} = Ch.start_link(Ch.Test.client_opts())

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             Ch.query(conn, "show settings like 'async_insert'")
  end

  test "can pass default settings" do
    assert {:ok, conn} = Ch.start_link(Ch.Test.client_opts(settings: [async_insert: 1]))

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}} =
             Ch.query(conn, "show settings like 'async_insert'")
  end

  test "can overwrite default settings with options" do
    assert {:ok, conn} = Ch.start_link(Ch.Test.client_opts(settings: [async_insert: 1]))

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             Ch.query(conn, "show settings like 'async_insert'", [], settings: [async_insert: 0])
  end
end
