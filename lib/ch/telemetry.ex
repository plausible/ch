defmodule Ch.Telemetry do
  @moduledoc """
  TODO
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
