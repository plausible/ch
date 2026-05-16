defmodule Help do
  @moduledoc false

  def setup_pool(%{pool: pool}) when is_pid(pool), do: :ok

  def setup_pool(test_context) do
    {:ok, pool: ExUnit.Callbacks.start_supervised!(Ch), session_id: session_id(test_context)}
  end

  def query!(test_context, statement, params \\ %{}, options \\ []) do
    %{pool: pool, session_id: session_id} = test_context
    session_settings = [session_id: session_id]

    options =
      Keyword.update(options, :settings, session_settings, fn settings ->
        Keyword.merge(session_settings, settings)
      end)

    Ch.query!(pool, statement, params, options)
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

  def ch(statement, params \\ %{}, options \\ []) do
    path = Ch.HTTP.path(params, options)
    url = Path.join("http://localhost:8123", path)

    http("POST", url,
      body: statement,
      headers: [{"x-clickhouse-format", "RowBinaryWithNamesAndTypes"}]
    )
  end

  def http(method, url, options \\ []) do
    %URI{scheme: scheme, host: host, port: port} = URI.parse(url)

    scheme =
      case scheme do
        "http" -> :http
        "https" -> :https
        other -> raise ArgumentError, "invalid scheme: #{inspect(other)}"
      end

    with {:ok, conn} <- Mint.HTTP1.connect(scheme, host, port) do
      try do
        with {:ok, conn, _ref} <- Mint.HTTP1.request() do
          http_recv_all(conn)
        end
      after
        Mint.HTTP1.close(conn)
      end
    end
  end
end
