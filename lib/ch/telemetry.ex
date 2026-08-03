defmodule Ch.Telemetry do
  @moduledoc false

  def query_start(metadata) do
    started = System.monotonic_time()
    execute([:ch, :query, :start], %{system_time: System.system_time()}, metadata)
    {started, event([], :encode)}
  end

  def query_stop(events, started, result, metadata) do
    execute([:ch, :query, :stop], measurements(events, started, result), metadata)
  end

  def query_error(events, started, metadata) do
    execute([:ch, :query, :error], measurements(events, started, nil), metadata)
  end

  def event(events, name), do: [{name, System.monotonic_time()} | events]

  def past_event(events, name, time), do: [{name, time} | events]

  def maybe_checkin_event(events, {:ok, _conn, checkin_time, _conn_metadata}) do
    past_event(events, :checkin, checkin_time)
  end

  def maybe_checkin_event(events, _conn_or_template), do: events

  def execute(event, measurements, metadata) do
    :telemetry.execute(event, measurements, metadata)
  end

  def duration(started), do: System.monotonic_time() - started

  defp measurements(events, started, result) do
    stop = System.monotonic_time()

    measurements =
      events
      |> Enum.reduce({stop, %{duration: stop - started}}, fn
        {:decode, start}, {stop, measurements} ->
          {start, Map.put(measurements, :decode_time, stop - start)}

        {:query, start}, {stop, measurements} ->
          {start, Map.put(measurements, :query_time, stop - start)}

        {:checkout, start}, {stop, measurements} ->
          {start, Map.put(measurements, :queue_time, stop - start)}

        {:checkin, start}, {stop, measurements} ->
          {stop, Map.put(measurements, :idle_time, max(stop - start, 0))}

        {:encode, start}, {stop, measurements} ->
          {start, Map.put(measurements, :encode_time, stop - start)}
      end)
      |> elem(1)

    case result do
      %Ch.Result{names: names, data: data} ->
        measurements
        |> maybe_put(:num_columns, names && length(names))
        |> maybe_put(:response_body_bytes, data && IO.iodata_length(data))

      _ ->
        measurements
    end
  end

  defp maybe_put(measurements, _name, nil), do: measurements
  defp maybe_put(measurements, name, value), do: Map.put(measurements, name, value)
end
