defmodule Ch.SettingsTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  test "can start without settings", %{query_options: query_options} do
    assert {:ok, conn} = Ch.start_link()

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             Ch.query(conn, "show settings like 'async_insert'", [], query_options)
  end

  test "can pass default settings", %{query_options: query_options} do
    assert {:ok, conn} = Ch.start_link(settings: [async_insert: 1])

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "1"]]}} =
             Ch.query(conn, "show settings like 'async_insert'", [], query_options)
  end

  test "can overwrite default settings with options", %{query_options: query_options} do
    assert {:ok, conn} = Ch.start_link(settings: [async_insert: 1])

    assert {:ok, %{num_rows: 1, rows: [["async_insert", "Bool", "0"]]}} =
             Ch.query(
               conn,
               "show settings like 'async_insert'",
               [],
               Keyword.merge(query_options, settings: [async_insert: 0])
             )
  end
end
