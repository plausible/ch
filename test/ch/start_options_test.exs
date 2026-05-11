defmodule Ch.StartOptionsTest do
  use ExUnit.Case, async: true

  test "rejects query/auth configuration at pool startup" do
    for option <- [:database, :username, :password] do
      assert_raise NimbleOptions.ValidationError, fn ->
        Ch.start_link([{option, "value"}])
      end
    end
  end
end
