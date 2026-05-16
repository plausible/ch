defmodule Ch.SettingsTest do
  use ExUnit.Case, async: true

  test "can pass settings in options" do
    pool = start_supervised!(Ch)

    assert Ch.query!(pool, "show settings like 'async_insert'", _params = %{},
             settings: %{"async_insert" => 1}
           ).rows == [
             ["async_insert", "Bool", "1"]
           ]

    assert Ch.query!(pool, "show settings like 'async_insert'", _params = %{},
             settings: %{"async_insert" => 0}
           ).rows == [
             ["async_insert", "Bool", "0"]
           ]
  end
end
