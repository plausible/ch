defmodule Ch.ConnectTest do
  use ExUnit.Case, async: true

  @tag :slow
  test "retries to connect even with exceptions" do
    {:ok, conn} =
      Ch.start_link(database: Ch.Test.database(), transport_opts: [sndbuf: nil])

    :timer.sleep(:timer.seconds(5))

    assert Process.alive?(conn)
  end
end
