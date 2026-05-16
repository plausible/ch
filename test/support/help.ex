defmodule Help do
  @moduledoc false

  def session_id(%{module: module, test: test}) do
    rand =
      Base.hex_encode32(
        <<
          System.system_time(:nanosecond)::64,
          :erlang.phash2(self(), 16_777_216)::24,
          :erlang.unique_integer()::32
        >>,
        case: :lower
      )

    "#{module}-#{test}-#{rand}"
  end
end
