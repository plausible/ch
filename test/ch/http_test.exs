defmodule Ch.HTTPTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  doctest Ch.HTTP

  property "timeout to deadline to timeout preserves the remaining timeout" do
    check all timeout <- integer(0..60_000) do
      round_tripped = timeout |> Ch.HTTP.to_deadline() |> Ch.HTTP.to_timeout()

      assert round_tripped <= timeout
      assert round_tripped >= max(timeout - 50, 0)
    end
  end

  property "deadline to timeout to deadline preserves the absolute deadline" do
    check all offset <- integer(0..60_000) do
      deadline = {:deadline, System.monotonic_time(:millisecond) + offset}
      {:deadline, original_timestamp} = deadline

      {:deadline, round_tripped_timestamp} =
        deadline |> Ch.HTTP.to_timeout() |> Ch.HTTP.to_deadline()

      assert_in_delta round_tripped_timestamp, original_timestamp, 50
    end
  end
end
