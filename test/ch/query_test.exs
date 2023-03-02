defmodule Ch.QueryTest do
  use ExUnit.Case, async: true

  test "to_string" do
    query = Ch.Query.build(["select ", 1 + ?0, ?+, 2 + ?0])
    assert to_string(query) == "select 1+2"
  end
end
