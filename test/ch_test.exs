defmodule ChTest do
  use ExUnit.Case
  doctest Ch

  test "greets the world" do
    assert Ch.hello() == :world
  end
end
