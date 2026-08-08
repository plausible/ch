defmodule Ch.Test do
  @moduledoc false

  @toxiproxy_counter {__MODULE__, :toxiproxy_counter}

  def database, do: Application.fetch_env!(:ch, :database)

  # makes a query in a short lived process so that pool automatically exits once finished
  def query(sql, params \\ [], opts \\ []) do
    task =
      Task.async(fn ->
        {:ok, pid} = Ch.start_link(opts)
        opts = Keyword.put_new_lazy(opts, :database, &database/0)
        Ch.query!(pid, sql, params, opts)
      end)

    Task.await(task)
  end

  # helper for ExUnit.Case :parameterize
  def parameterize_query_options(ctx, options \\ []) do
    if default_options = ctx[:query_options] do
      Keyword.merge(default_options, options)
    else
      options
    end
  end

  def parameterize_query(ctx, sql, params \\ [], options \\ []) do
    Ch.query(
      ctx.conn,
      sql,
      params,
      parameterize_query_options(ctx, options)
    )
  end

  def parameterize_query!(ctx, sql, params \\ [], options \\ []) do
    Ch.query!(
      ctx.conn,
      sql,
      params,
      parameterize_query_options(ctx, options)
    )
  end

  def setup_toxiproxy_counter do
    :persistent_term.put(@toxiproxy_counter, :atomics.new(1, signed: false))
  end

  def create_toxiproxy(upstream) do
    index =
      @toxiproxy_counter
      |> :persistent_term.get()
      |> :atomics.add_get(1, 1)

    name = "ch_test-#{index}"
    port = 8474 + index

    toxiproxy(:post, "/proxies", %{
      "name" => name,
      "listen" => "0.0.0.0:#{port}",
      "upstream" => upstream,
      "enabled" => true
    })

    ExUnit.Callbacks.on_exit(fn -> toxiproxy(:delete, "/proxies/#{name}") end)
    %{name: name, port: port}
  end

  def toxiproxy_up(%{name: name}) do
    toxiproxy(:post, "/proxies/#{name}", %{"enabled" => true})
  end

  def toxiproxy_down(%{name: name}) do
    toxiproxy(:post, "/proxies/#{name}", %{"enabled" => false})
  end

  def add_toxic(%{name: name}, %{"name" => _toxic_name} = toxic) do
    toxiproxy(:post, "/proxies/#{name}/toxics", toxic)
  end

  def remove_toxic(%{name: name}, toxic_name) do
    toxiproxy(:delete, "/proxies/#{name}/toxics/#{toxic_name}")
  end

  def toxiproxy(method, path, data \\ nil) do
    url = ~c"http://localhost:8474#{path}"

    request =
      if data do
        {url, [], ~c"application/json", Jason.encode!(data)}
      else
        {url, []}
      end

    case :httpc.request(method, request, [], body_format: :binary) do
      {:ok, {{_version, status, _reason}, _headers, _body}} when status in 200..299 ->
        :ok

      {:ok, {{_version, status, _reason}, _headers, body}} ->
        raise "unexpected status #{status} from Toxiproxy: #{body}"

      {:error, reason} ->
        raise "Toxiproxy request failed: #{inspect(reason)}"
    end
  end

  # TODO packet: :http?
  def intercept_packets(socket, buffer \\ <<>>) do
    receive do
      {:tcp, ^socket, packet} ->
        buffer = buffer <> packet

        if complete?(buffer) do
          buffer
        else
          intercept_packets(socket, buffer)
        end
    end
  end

  defp complete?(buffer) do
    with {:ok, rest} <- eat_status(buffer),
         {:ok, content_length, rest} <- eat_headers(rest) do
      verify_body(content_length, rest)
    else
      _ -> false
    end
  end

  defp eat_status(buffer) do
    case :erlang.decode_packet(:http_bin, buffer, []) do
      {:ok, _, rest} -> {:ok, rest}
      {:more, _} -> {:more, buffer}
    end
  end

  defp eat_headers(buffer, content_length \\ nil) do
    case :erlang.decode_packet(:httph_bin, buffer, []) do
      {:ok, {_, _, :"Content-Length", _, content_length}, rest} ->
        eat_headers(rest, String.to_integer(content_length))

      {:ok, {_, _, :"Transfer-Encoding", _, "chunked"}, rest} ->
        eat_headers(rest, :chunked)

      {:ok, :http_eoh, rest} ->
        {:ok, content_length, rest}

      {:ok, _, rest} ->
        eat_headers(rest, content_length)

      {:more, _} ->
        {:more, buffer}
    end
  end

  defp verify_body(:chunked, chunks) do
    String.ends_with?(chunks, "\r\n0\r\n\r\n")
  end

  defp verify_body(content_length, body) do
    byte_size(body) == content_length
  end

  # shifts naive datetimes for non-utc timezones into utc to match ClickHouse behaviour
  # see https://clickhouse.com/docs/en/sql-reference/data-types/datetime#usage-remarks
  def to_clickhouse_naive(conn, %NaiveDateTime{} = naive_datetime) do
    case Ch.query!(conn, "select timezone()").rows do
      [["UTC"]] ->
        naive_datetime

      [[timezone]] ->
        naive_datetime
        |> DateTime.from_naive!(timezone)
        |> DateTime.shift_zone!("Etc/UTC")
        |> DateTime.to_naive()
    end
  end

  def clickhouse_tz(conn) do
    case Ch.query!(conn, "select timezone()").rows do
      [["UTC"]] -> "Etc/UTC"
      [[timezone]] -> timezone
    end
  end
end
