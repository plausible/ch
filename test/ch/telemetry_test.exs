defmodule Ch.TelemetryTest do
  use ExUnit.Case, async: true

  setup do
    pool = start_supervised!(Ch)
    handler_id = {__MODULE__, self(), System.unique_integer()}

    events = [
      [:ch, :query, :start],
      [:ch, :query, :stop],
      [:ch, :query, :error],
      [:ch, :conn, :start],
      [:ch, :conn, :stop],
      [:ch, :conn, :reuse],
      [:ch, :conn, :drop],
      [:ch, :conn, :error]
    ]

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, pool: pool}
  end

  test "emits query and connection telemetry for successful queries", %{pool: pool} do
    assert {:ok, %Ch.Result{rows: [[1]]}} =
             Ch.query(pool, "SELECT 1", %{}, telemetry_metadata: %{source: :test})

    assert_receive {:telemetry_event, [:ch, :query, :start], %{system_time: system_time},
                    %{telemetry_metadata: %{source: :test}, format: "RowBinaryWithNamesAndTypes"}}

    assert is_integer(system_time)

    assert_receive {:telemetry_event, [:ch, :conn, :start], %{system_time: conn_system_time},
                    %{scheme: :http, host: "localhost", port: 8123}}

    assert is_integer(conn_system_time)

    assert_receive {:telemetry_event, [:ch, :conn, :stop], %{duration: conn_duration},
                    %{scheme: :http, host: "localhost", port: 8123}}

    assert is_integer(conn_duration)
    assert conn_duration >= 0

    assert_receive {:telemetry_event, [:ch, :query, :stop], measurements,
                    %{status: 200, telemetry_metadata: %{source: :test}, result: %Ch.Result{}}}

    assert measurements.duration >= 0
    assert measurements.queue_time >= 0
    assert measurements.query_time >= 0
    assert measurements.decode_time >= 0
    assert measurements.num_rows == 1
    assert measurements.num_columns == 1
    assert measurements.response_body_bytes > 0

    assert {:ok, %Ch.Result{rows: [[2]]}} = Ch.query(pool, "SELECT 2")

    assert_receive {:telemetry_event, [:ch, :conn, :reuse], %{idle_time: idle_time},
                    %{scheme: :http, host: "localhost", port: 8123}}

    assert idle_time >= 0
  end

  test "emits query error telemetry for ClickHouse errors", %{pool: pool} do
    assert {:error, %Ch.Error{} = error} = Ch.query(pool, "SELECT missing_column")

    assert_receive {:telemetry_event, [:ch, :query, :error], measurements,
                    %{
                      kind: :error,
                      reason: ^error,
                      clickhouse_error_code: code,
                      status: status
                    }}

    assert measurements.duration >= 0
    assert measurements.queue_time >= 0
    assert measurements.query_time >= 0
    assert measurements.decode_time >= 0
    assert is_integer(code)
    assert status != 200
  end

  def handle_event(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, event, measurements, metadata})
  end
end
