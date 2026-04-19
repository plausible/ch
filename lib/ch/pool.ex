defmodule Ch.Pool do
  @moduledoc """
  TODO
  """

  use GenServer

  @type statement :: iodata
  @type params :: %{String.t() => term}

  @query_timeout to_timeout(second: 30)

  # TODO
  @type query_result :: term
  @type query_error :: Ch.Error.t() | Mint.Types.error()

  @start_options_schema [
    name: [
      type: :any,
      doc: "Process name registration (e.g. `MyPool` or `{:via, Registry, :ch}`)."
    ],
    pool_size: [
      type: :pos_integer,
      doc: "Maximum number of concurrent connections.",
      default: 10
    ],
    url: [
      type: :string,
      doc: "The ClickHouse endpoint URL.",
      default: "http://localhost:8123"
    ]
  ]

  @typedoc """
  The options supported by `start_link/1`.
  """
  @type start_option :: unquote(NimbleOptions.option_typespec(@start_options_schema))

  @doc """
  Starts a new Ch pool process.

  Supported options:
  #{NimbleOptions.docs(@start_options_schema)}
  """
  @spec start_link(keyword) :: GenServer.on_start()
  def start_link(options) do
    options = NimbleOptions.validate!(options, @start_options_schema)

    name = Keyword.get(options, :name)
    pool_size = Keyword.fetch!(options, :pool_size)
    url = Keyword.fetch!(options, :url)

    %URI{scheme: scheme, host: host, port: port} = URI.parse(url)

    scheme =
      case scheme do
        "http" -> :http
        "https" -> :https
        _other -> raise ArgumentError, "unexpected HTTP scheme: #{inspect(scheme)}"
      end

    config = [
      pool_size: pool_size,
      template: {scheme, host, port}
    ]

    GenServer.start_link(__MODULE__, config, name: name)
  end

  @doc """
  Stops the given `pool`.

  The pool exits with the given `reason`. The pool has `timeout` milliseconds to stop
  before it's unilaterally killed by the runtime.
  """
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pool, reason, timeout)
  end

  @spec query(NimblePool.pool(), statement, params, keyword) ::
          {:ok, query_result} | {:error, query_error}
  def query(pool, statement, params \\ %{}, options \\ []) do
    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)

    deadline = Ch.HTTP.to_deadline(timeout)
    path = Ch.HTTP.path(params, options)

    # TODO retry on closed? backoff?
    # TODO retry transient closed/etc. errors?
    checkout(pool, timeout, fn ref, conn ->
      with {:ok, conn} <- ensure_connected(conn, pool, deadline),
           {:ok, conn, result} <- request(conn, path, statement, deadline) do
        {result, checkin(conn)}
      else
        {:error, reason} = error -> {error, {:remove, reason}}
      end
    end)
  end

  @spec query!(NimblePool.pool(), statement, params, keyword) :: query_result
  def query!(pool, statement, params \\ %{}, options \\ []) do
    case query(pool, statement, params, options) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  defp checkout(pool, timeout, fun) when is_function(fun) do
    monitor_ref = Process.monitor(pool)

    # TODO noconnect?
    GenServer.cast(pool, {:out, self(), monitor_ref, timeout})

    receive do
      {^monitor_ref, conn, request_ref} ->
        Process.demonitor(monitor_ref, [:flush])
        {result, conn} = fun.(conn)
        GenServer.cast(pool, {:in, conn, request_ref})
        result

      {^monitor_ref, :timeout} ->
        Process.demonitor(monitor_ref, [:flush])
        {:error, :timeout}

      {:DOWN, ^monitor_ref, :process, _pid, reason} ->
        {:error, reason}
    end
  end

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)

    pool_size = Keyword.fetch!(config, :pool_size)
    template = Keyword.fetch!(config, :template)

    state = %{
      queue: :queue.new(),
      requests: %{},
      monitors: %{},
      resources: :queue.new(),
      pool_size: pool_size,
      template: template
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:out, pid, request_ref, timeout}, state) do
    monitor_ref = Process.monitor(pid)

    %{requests: requests, monitors: monitors} = state
    requests = Map.put(requests, request_ref, {pid, monitor_ref, :out})
    monitors = Map.put(monitors, monitor_ref, request_ref)
    state = %{state | requests: requests, monitors: monitors}
    state = maybe_checkout(request_ref, monitor_ref, timeout, pid, state)

    {:noreply, state}
  end

  def handle_cast({:in, conn, monitor_ref}, state) do
    Process.demonitor(monitor_ref, [:flush])

    %{requests: requests, resources: resources} = state

    resources =
      case handle_checkin(conn) do
        {:ok, conn} ->
          :queue.in(conn, resources)

        {:remove, reason} ->
          remove_worker(reason, conn)
          resources
      end

    state = remove_requests(state, monitor_ref)
    state = maybe_checkout(%{state | resources: resources})
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, _, _, _} = down, state) do
  end

  def handle_info({:ping, worker}, state) do
  end

  @impl GenServer
  def terminate(reason, state) do
  end

  defp maybe_checkout(%{queue: queue, requests: requests} = state) do
    case :queue.out(queue) do
      {{:value, {pid, ref}, queue}} ->
        case requests do
          # the request still exists, so we can checkout the resource
          %{^ref => {^pid, mon_ref, :out, deadline}} ->
            maybe_checkout(command, mon_ref, deadline, {pid, ref}, %{state | queue: queue})

          # it should never happen
          %{^ref => _} ->
            exit(:unexpected_checkout)

          # the request is no longer active, try the next one
          %{} ->
            maybe_checkout(%{state | queue: queue})
        end

      {:empty, _queue} ->
        state
    end
  end

  defp handle_checkin({:ok, %Mint.HTTP1{} = conn}, _from, _conn, config) do
    {:ok, conn, config}
  end

  defp handle_checkin({:remove, reason}, _from, _conn, config) do
    {:remove, reason, config}
  end

  defp handle_checkout(:request, _from, :template = template, config) do
    {:ok, config.template, template, config}
  end

  defp handle_checkout(:request, _from, %Mint.HTTP1{} = conn, config) do
    {:ok, {:ok, conn}, conn, config}
  end

  defp handle_ping(_conn, _config) do
    {:remove, :worker_idle_timeout}
  end

  # TODO handle_info

  @impl NimblePool
  def terminate_worker(_reason, conn, config) do
    with %Mint.HTTP1{} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, config}
  end

  defp ensure_connected({:template, scheme, host, port}, owner, deadline) do
    timeout = Ch.HTTP.timeout_from_deadline(deadline)

    case Mint.HTTP1.connect(scheme, host, port, mode: :passive, timeout: timeout) do
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

  defp ensure_connected({:ok, %Mint.HTTP1{}} = ok, _owner, _deadline), do: ok

  defp checkin(conn) do
    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:remove, Mint.TransportError.exception(reason: :closed)}
    end
  end
end
