defmodule Ch.ConnectTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  @tag :slow
  test "retries to connect even with exceptions / exits / throws" do
    # See https://github.com/plausible/ch/issues/208
    bad_transport_opts = [sndbuf: nil]

    logs =
      capture_log(fn ->
        {:ok, conn} =
          Ch.start_link(Ch.Test.client_opts(transport_opts: bad_transport_opts))

        :timer.sleep(100)

        assert Process.alive?(conn)
      end)

    assert logs =~ "failed to connect: ** (ArgumentError) argument error"
  end
end
