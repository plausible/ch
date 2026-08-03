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
                    %{pool: ^pool, source: :test, format: "RowBinaryWithNamesAndTypes"}}

    assert is_integer(system_time)

    assert_receive {:telemetry_event, [:ch, :conn, :start], %{system_time: conn_system_time},
                    %{pool: ^pool, scheme: :http, host: "localhost", port: 8123}}

    assert is_integer(conn_system_time)

    assert_receive {:telemetry_event, [:ch, :conn, :stop], %{duration: conn_duration},
                    %{pool: ^pool, scheme: :http, host: "localhost", port: 8123}}

    assert is_integer(conn_duration)
    assert conn_duration >= 0

    assert_receive {:telemetry_event, [:ch, :query, :stop], measurements,
                    %{pool: ^pool, status: 200, source: :test, result: %Ch.Result{}}}

    assert measurements.duration >= 0
    assert measurements.encode_time >= 0
    assert measurements.queue_time >= 0
    assert measurements.query_time >= 0
    assert measurements.decode_time >= 0
    assert measurements.num_columns == 1
    assert measurements.response_body_bytes > 0

    assert {:ok, %Ch.Result{rows: [[2]]}} = Ch.query(pool, "SELECT 2")

    assert_receive {:telemetry_event, [:ch, :conn, :reuse], %{idle_time: idle_time},
                    %{pool: ^pool, scheme: :http, host: "localhost", port: 8123}}

    assert idle_time >= 0

    assert_receive {:telemetry_event, [:ch, :query, :stop], reused_measurements,
                    %{pool: ^pool, status: 200}}

    assert reused_measurements.idle_time >= 0
  end

  test "emits query error telemetry for ClickHouse errors", %{pool: pool} do
    assert {:error, %Ch.Error{} = error} = Ch.query(pool, "SELECT missing_column")

    assert_receive {:telemetry_event, [:ch, :query, :error], measurements,
                    %{
                      kind: :error,
                      pool: ^pool,
                      reason: ^error,
                      clickhouse_error_code: code,
                      status: status
                    }}

    assert measurements.duration >= 0
    assert measurements.encode_time >= 0
    assert measurements.queue_time >= 0
    assert measurements.query_time >= 0
    assert measurements.decode_time >= 0
    assert is_integer(code)
    assert status != 200
  end

  test "emits connection error telemetry when opening a connection fails" do
    stop_supervised(Ch)
    pool = start_supervised!({Ch, url: "http://localhost:1"})

    assert {:error, reason} = Ch.query(pool, "SELECT 1", %{}, timeout: 100)

    assert_receive {:telemetry_event, [:ch, :conn, :error], %{duration: duration},
                    %{pool: ^pool, kind: :error, reason: ^reason}}

    assert duration >= 0
  end

  test "emits connection drop telemetry when a pooled connection is removed", %{pool: pool} do
    assert {:ok, %Ch.Result{}} = Ch.query(pool, "SELECT 1")
    assert :ok = stop_supervised(Ch)

    assert_receive {:telemetry_event, [:ch, :conn, :drop], %{}, %{pool: ^pool, reason: _reason}}
  end

  test "emits query error telemetry when pool checkout times out" do
    stop_supervised(Ch)
    pool = start_supervised!({Ch, pool_size: 1})
    task = Task.async(fn -> Ch.query(pool, "SELECT sleep(0.1)") end)

    assert_receive {:telemetry_event, [:ch, :conn, :stop], _measurements, %{pool: ^pool}}

    reason = catch_exit(Ch.query(pool, "SELECT 1", %{}, timeout: 0))

    assert_receive {:telemetry_event, [:ch, :query, :error], measurements,
                    %{pool: ^pool, kind: :exit, reason: ^reason, stacktrace: stacktrace}}

    assert measurements.duration >= 0
    assert measurements.encode_time >= 0
    assert measurements.queue_time >= 0
    assert is_list(stacktrace)
    assert {:ok, %Ch.Result{}} = Task.await(task)
  end

  test "emits drop telemetry when a checked-out connection is terminated" do
    config = %{template: {:template, :http, "localhost", 8123}, pool: self()}
    conn = %Mint.HTTP1{}

    assert {:ok, ^config} = Ch.terminate_worker(:error, conn, config)

    assert_receive {:telemetry_event, [:ch, :conn, :drop], %{}, %{pool: test_pid, reason: :error}}

    assert test_pid == self()
  end

  def handle_event(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, event, measurements, metadata})
  end
end
