defmodule Ch.Pool do
  @moduledoc """
  TODO
  """

  @behaviour NimblePool

  @type statement :: iodata
  @type params :: %{String.t() => term}

  @pool_size 10
  @worker_idle_timeout to_timeout(second: 5)
  @query_timeout to_timeout(second: 30)

  # TODO
  @type query_result :: term
  @type query_error :: Ch.Error.t() | Mint.Types.error()

  # TODO nimble options, todo can pass settings
  @spec start_link(keyword) :: GenServer.on_start()
  def start_link(options) do
    {name, options} = Keyword.pop(options, :name)
    {pool_size, options} = Keyword.pop(options, :pool_size, @pool_size)

    {worker_idle_timeout, options} =
      Keyword.pop(options, :worker_idle_timeout, @worker_idle_timeout)

    NimblePool.start_link(
      worker: {__MODULE__, options},
      pool_size: pool_size,
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: name
    )
  end

  @spec child_spec(keyword) :: Supervisor.child_spec()
  def child_spec(options) do
    options
    |> Keyword.put(:worker, {__MODULE__, options})
    |> NimblePool.child_spec()
  end

  @spec query(NimblePool.pool(), statement, params, keyword) ::
          {:ok, query_result} | {:error, query_error}
  def query(pool, statement, params \\ %{}, options \\ []) do
    request = Ch.HTTP.encode_request("POST", statement, params, options)

    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)
    deadline = Ch.HTTP.deadline_from_timeout(timeout)

    # TODO retry on closed
    result =
      NimblePool.checkout!(
        pool,
        :request,
        fn {pid, _ref}, conn ->
          # TODO what if caller dies? does nimble pool terminate the worker? probably
          # TODO retry transient closed/etc. errors?
          with {:ok, conn} <- ensure_connected(conn, pid, deadline),
               {:ok, conn, response} <- Ch.HTTP.request(conn, request, deadline) do
            {{:ok, response}, checkin(conn)}
          else
            {:error, reason} = error -> {error, {:remove, reason}}
          end
        end,
        timeout
      )

    with {:ok, response} <- result do
      Ch.HTTP.decode_response(response, options)
    end
  end

  @spec query!(NimblePool.pool(), statement, params, keyword) :: query_result
  def query!(pool, statement, params \\ %{}, options \\ []) do
    case query(pool, statement, params, options) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec stop(NimblePool.pool(), reason :: term, timeout) :: :ok
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    NimblePool.stop(pool, reason, timeout)
  end

  @impl NimblePool
  def init_pool(options) do
    scheme = Keyword.fetch!(options, :scheme)
    host = Keyword.fetch!(options, :host)
    port = Keyword.fetch!(options, :port)

    transport_options =
      options
      |> Keyword.get(:transport_options, [])
      |> Keyword.put(:mode, :passive)

    config = %{
      scheme: scheme,
      host: host,
      port: port,
      transport_options: transport_options
    }

    {:ok, config}
  end

  @impl NimblePool
  def init_worker(config) do
    %{scheme: scheme, host: host, port: port, transport_options: options} = config
    {:ok, {:idle, scheme, host, port, options}, config}
  end

  @impl NimblePool
  def handle_checkout(:request, _from, conn, config) do
    {:ok, conn, conn, config}
  end

  @impl NimblePool
  def handle_checkin({:ok, conn}, _from, _conn, config) do
    {:ok, {:connected, conn}, config}
  end

  def handle_checkin({:remove, reason}, _from, _conn, config) do
    {:remove, reason, config}
  end

  @impl NimblePool
  def handle_ping(_conn, _config) do
    {:remove, :idle_timeout}
  end

  # TODO handle_info?

  @impl NimblePool
  def terminate_worker(_reason, conn, config) do
    with {:connected, conn} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, config}
  end

  defp ensure_connected({:idle, scheme, host, port, options}, owner, deadline) do
    timeout = Ch.HTTP.timeout_from_deadline(deadline)
    options = Keyword.put(options, :timeout, timeout)

    case Mint.HTTP1.connect(scheme, host, port, options) do
      {:ok, conn} ->
        case Mint.HTTP1.controlling_process(conn, owner) do
          {:ok, _conn} = ok ->
            ok

          {:error, _reason} = error ->
            Mint.HTTP1.close(conn)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_connected({:connected, conn}, _owner, _deadline), do: {:ok, conn}

  defp checkin(conn) do
    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:remove, Mint.TransportError.exception(reason: :closed)}
    end
  end
end
