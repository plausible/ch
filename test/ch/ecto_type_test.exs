defmodule Ch.EctoTypeTest do
  use ExUnit.Case, async: true

  test "it works" do
    assert {:parameterized, Ch, :string} = type = Ecto.ParameterizedType.init(Ch, type: "String")

    assert Ecto.Type.type(type) == type

    assert {:ok, "something"} = Ecto.Type.cast(type, "something")
    assert {:ok, "something"} = Ecto.Type.dump(type, "something")
    assert {:ok, "something"} = Ecto.Type.load(type, "something")
  end
end
