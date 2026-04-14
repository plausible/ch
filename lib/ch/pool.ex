defmodule Ch.Pool do
  @moduledoc """
  TODO
  """

  @behaviour NimblePool

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
    worker_idle_timeout: [
      type: :timeout,
      doc: """
      Time a connection can stay idle before the pool closes it.
      Should be lower than ClickHouse's `keep_alive_timeout`.
      """,
      default: to_timeout(second: 5)
    ],
    url: [
      type: :string,
      doc: "The ClickHouse endpoint URL.",
      default: "http://localhost:8123"
    ],
    connect_options: [
      type: :keyword_list,
      default: [],
      doc: "Options passed to `Mint.HTTP.connect/4` (e.g. `:timeout`, `:proxy`)."
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
    worker_idle_timeout = Keyword.fetch!(options, :worker_idle_timeout)
    url = Keyword.fetch!(options, :url)

    connect_options =
      options
      |> Keyword.get(:connect_options, [])
      |> Keyword.put(:mode, :passive)

    %URI{scheme: scheme, host: host, port: port} = URI.parse(url)

    scheme =
      case scheme do
        "http" -> :http
        "https" -> :https
        _other -> raise ArgumentError, "unexpected HTTP scheme: #{inspect(scheme)}"
      end

    initial_pool_state = %{
      template: {:template, scheme, host, port, connect_options}
    }

    NimblePool.start_link(
      worker: {__MODULE__, initial_pool_state},
      pool_size: pool_size,
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: name
    )
  end

  @doc """
  Returns a child spec to allow Ch pool to be started under a supervisor.

  ## Options

  The options are exactly the same as for `start_link/1`.
  """
  @spec child_spec(keyword) :: Supervisor.child_spec()
  def child_spec(options) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [options]}}
  end

  @spec query(NimblePool.pool(), statement, params, keyword) ::
          {:ok, query_result} | {:error, query_error}
  def query(pool, statement, params \\ %{}, options \\ []) do
    request = Ch.HTTP.encode_request("POST", statement, params, options)

    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)
    deadline = Ch.HTTP.deadline_from_timeout(timeout)

    # TODO retry on closed? backoff?
    result =
      NimblePool.checkout!(
        pool,
        :request,
        fn {pid, _ref}, conn ->
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
  def init_pool(config) do
    {:ok, config}
  end

  @impl NimblePool
  def init_worker(config) do
    {:ok, :template, config}
  end

  @impl NimblePool
  def handle_checkout(:request, _from, :template = template, config) do
    {:ok, config.template, template, config}
  end

  def handle_checkout(:request, _from, %Mint.HTTP1{} = conn, config) do
    {:ok, {:ok, conn}, conn, config}
  end

  @impl NimblePool
  def handle_checkin({:ok, %Mint.HTTP1{} = conn}, _from, _conn, config) do
    {:ok, conn, config}
  end

  def handle_checkin({:remove, reason}, _from, _conn, config) do
    {:remove, reason, config}
  end

  @impl NimblePool
  def handle_ping(_conn, _config) do
    {:remove, :worker_idle_timeout}
  end

  # TODO handle_info?

  @impl NimblePool
  def terminate_worker(_reason, conn, config) do
    with %Mint.HTTP1{} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, config}
  end

  defp ensure_connected({:template, scheme, host, port, options}, owner, deadline) do
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

  defp ensure_connected({:ok, %Mint.HTTP1{}} = ok, _owner, _deadline), do: ok

  defp checkin(conn) do
    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:remove, Mint.TransportError.exception(reason: :closed)}
    end
  end
end
