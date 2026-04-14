defmodule Help do
  @moduledoc false

  def start_supervised_pool!(_test_context) do
    ExUnit.Callbacks.start_supervised!({Ch.Pool, scheme: :http, host: "localhost", port: 8123})
  end

  def setup_pool(%{pool: pool}) when is_pid(pool), do: :ok

  def setup_pool(test_context) do
    {:ok, pool: start_supervised_pool!(test_context), session_id: session_id(test_context)}
  end

  def query!(test_context, statement, params \\ %{}, options \\ []) do
    %{pool: pool, session_id: session_id} = test_context
    session_settings = [session_id: session_id]

    options =
      Keyword.update(options, :settings, session_settings, fn settings ->
        Keyword.merge(session_settings, settings)
      end)

    Ch.Pool.query!(pool, statement, params, options)
  end

  def session_id(test_context) do
    %{module: module, test: test} = test_context

    rand =
      Base.hex_encode32(
        <<
          System.system_time(:nanosecond)::64,
          :erlang.phash2({node(), self()}, 16_777_216)::24,
          :erlang.unique_integer()::32
        >>,
        case: :lower
      )

    "#{module}-#{test}-#{rand}"
  end
end
