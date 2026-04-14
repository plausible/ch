defmodule Ch.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  A complete list of emitted events is available in the [Telemetry Events](telemetry-events.md) reference.

  ### Default Logging Handler

  Xinesis provides a default Telemetry handler that logs connection and request events at appropriate log levels.
  To enable this default logging, call `attach_default_handler/0`, to disable it, call `detach_default_handler/0`.
  """

  @default_handler_id "ch-default-handler"

  # def attach_default_handler do
  #   :telemetry.attach_many(@default_handler_id, [[]], &__MODULE__.handle_event/4, _no_config = [])
  # end

  def detach_default_handler do
    :telemetry.detach(@default_handler_id)
  end

  @doc false
  def handle_event([:ch | event], _measurements, metadata, _config) do
    case {event, metadata} do
      {[:connect, _stop_or_exception], %{kind: _, reason: _}} ->
        :ok
    end
  end
end
