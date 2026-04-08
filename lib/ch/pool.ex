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
    request = encode_request("POST", statement, params, options)

    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)
    deadline = deadline_from_timeout(timeout)

    # TODO retry on closed
    result =
      NimblePool.checkout!(
        pool,
        :request,
        fn {pid, _ref}, conn ->
          # TODO what if caller dies? does nimble pool terminate the worker? probably
          # TODO retry transient closed/etc. errors?
          with {:ok, conn} <- ensure_connected(conn, pid, deadline),
               {:ok, conn, response} <- request(conn, request, deadline) do
            {{:ok, response}, checkin(conn)}
          else
            {:error, reason} = error -> {error, {:remove, reason}}
          end
        end,
        timeout_from_deadline(deadline)
      )

    with {:ok, response} <- result do
      decode_response(response, options)
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

  # TODO handle_info

  @impl NimblePool
  def terminate_worker(_reason, conn, config) do
    with {:connected, conn} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, config}
  end

  defp deadline_from_timeout(:infinity = inf), do: inf

  defp deadline_from_timeout(timeout) do
    System.monotonic_time(:millisecond) + timeout
  end

  defp timeout_from_deadline(:infinity = inf), do: inf

  defp timeout_from_deadline(deadline) do
    max(0, deadline - System.monotonic_time(:millisecond))
  end

  defp ensure_connected({:idle, scheme, host, port, options}, owner, deadline) do
    timeout = timeout_from_deadline(deadline)
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

  defp encode_request(method, statement, params, options) do
    settings = Keyword.get(options, :settings, [])

    headers =
      options
      |> Keyword.get(:headers, [])
      |> put_new_header("x-clickhouse-format", "RowBinaryWithNamesAndTypes")

    path = "/?" <> URI.encode_query(settings ++ encode_params(params))
    %{method: method, path: path, headers: headers, body: statement}
  end

  defp request(conn, request, deadline) do
    %{method: method, path: path, headers: headers, body: body} = request

    case Mint.HTTP1.request(conn, method, path, headers, body) do
      {:ok, conn, _ref} ->
        receive_response(conn, [], deadline)

      {:error, conn, reason} ->
        _todo = Mint.HTTP1.close(conn)
        {:error, reason}
    end
  end

  defp receive_response(conn, acc, deadline) do
    timeout = timeout_from_deadline(deadline)

    case Mint.HTTP1.recv(conn, 0, timeout) do
      {:ok, conn, fragments} ->
        case handle_response_fragments(fragments, acc) do
          {:ok, response} -> {:ok, conn, response}
          {:more, acc} -> receive_response(conn, acc, deadline)
        end

      {:error, conn, reason, _fragments} ->
        _todo = Mint.HTTP1.close(conn)
        {:error, reason}
    end
  end

  for tag <- [:data, :status, :headers] do
    defp handle_response_fragments([{unquote(tag), _ref, data} | rest], acc) do
      handle_response_fragments(rest, [data | acc])
    end
  end

  defp handle_response_fragments([{:done, _ref}], acc), do: {:ok, :lists.reverse(acc)}
  defp handle_response_fragments([], acc), do: {:more, acc}

  defp checkin(conn) do
    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:remove, Mint.TransportError.exception(reason: :closed)}
    end
  end

  defp decode_response(response, _options) do
    case response do
      [200, headers | data] ->
        result =
          case get_header(headers, "x-clickhouse-format") do
            "RowBinaryWithNamesAndTypes" ->
              [names | rows] =
                data
                |> IO.iodata_to_binary()
                |> Ch.RowBinary.decode_names_and_rows()

              %{columns: names, rows: rows}

            _other ->
              %{data: data}
          end

        {:ok, result}

      [_status, headers | data] ->
        message = IO.iodata_to_binary(data)

        code =
          if code = get_header(headers, "x-clickhouse-exception-code") do
            String.to_integer(code)
          end

        {:error, Ch.Error.exception(code: code, message: message)}
    end
  end

  defp put_new_header(headers, name, value) do
    if List.keymember?(headers, name, 0) do
      headers
    else
      [{name, value} | headers]
    end
  end

  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil = not_found -> not_found
    end
  end

  defp encode_params(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)

  defp encode_param(b) when is_binary(b) do
    escape_param([{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}], b)
  end

  defp encode_param(b) when is_boolean(b), do: Atom.to_string(b)
  defp encode_param(nil), do: "\\N"
  defp encode_param(%Decimal{} = d), do: Decimal.to_string(d, :normal)
  defp encode_param(%Date{} = date), do: Date.to_iso8601(date)
  defp encode_param(%NaiveDateTime{} = naive), do: NaiveDateTime.to_iso8601(naive)
  defp encode_param(%Time{} = time), do: Time.to_iso8601(time)

  defp encode_param(%DateTime{microsecond: microsecond} = dt) do
    dt = DateTime.shift_zone!(dt, "Etc/UTC")

    case microsecond do
      {val, precision} when val > 0 and precision > 0 ->
        size = round(:math.pow(10, precision))
        unix = DateTime.to_unix(dt, size)
        seconds = div(unix, size)
        fractional = rem(unix, size)

        IO.iodata_to_binary([
          Integer.to_string(seconds),
          ?.,
          String.pad_leading(Integer.to_string(fractional), precision, "0")
        ])

      _ ->
        dt |> DateTime.to_unix(:second) |> Integer.to_string()
    end
  end

  defp encode_param(tuple) when is_tuple(tuple) do
    IO.iodata_to_binary([?(, encode_array_params(Tuple.to_list(tuple)), ?)])
  end

  defp encode_param(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_params(a), ?]])
  end

  defp encode_param(m) when is_map(m) do
    IO.iodata_to_binary([?{, encode_map_params(Map.to_list(m)), ?}])
  end

  defp encode_array_params([last]), do: encode_array_param(last)

  defp encode_array_params([s | rest]) do
    [encode_array_param(s), ?, | encode_array_params(rest)]
  end

  defp encode_array_params([] = empty), do: empty

  defp encode_map_params([last]), do: encode_map_param(last)

  defp encode_map_params([kv | rest]) do
    [encode_map_param(kv), ?, | encode_map_params(rest)]
  end

  defp encode_map_params([] = empty), do: empty

  defp encode_array_param(s) when is_binary(s) do
    [?', escape_param([{"'", "''"}, {"\\", "\\\\"}], s), ?']
  end

  defp encode_array_param(nil), do: "null"

  defp encode_array_param(%s{} = param) when s in [Date, NaiveDateTime] do
    [?', encode_param(param), ?']
  end

  defp encode_array_param(v), do: encode_param(v)

  defp encode_map_param({k, v}) do
    [encode_array_param(k), ?:, encode_array_param(v)]
  end

  defp escape_param([{pattern, replacement} | escapes], param) do
    param = String.replace(param, pattern, replacement)
    escape_param(escapes, param)
  end

  defp escape_param([], param), do: param
end
