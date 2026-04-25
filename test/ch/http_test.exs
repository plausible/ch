defmodule Ch.HTTPTest do
  use ExUnit.Case, async: true

  test "path encodes map params and settings" do
    assert Ch.HTTP.path(%{"city" => "Prague"}, %{max_threads: 1}) ==
             "/?param_city=Prague&max_threads=1"
  end

  test "path encodes keyword params and settings" do
    assert Ch.HTTP.path([city: "Prague"], query_id: "abc") ==
             "/?param_city=Prague&query_id=abc"
  end
end
