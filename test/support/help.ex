defmodule Help do
  @moduledoc false

  @pool Ch.TestPool
  @toxiproxy_counter {__MODULE__, :toxiproxy_counter}

  def session_id(%{module: module, test: test}) do
    rand =
      Base.hex_encode32(
        <<
          System.system_time(:nanosecond)::64,
          :erlang.phash2(self(), 16_777_216)::24,
          :erlang.unique_integer()::32
        >>,
        case: :lower
      )

    "#{module}-#{test}-#{rand}"
  end

  def start_link_pool(url) do
    Ch.start_link(name: @pool, url: url, pool_size: 100)
  end

  def query(statement, params \\ %{}, options \\ []) do
    Ch.query(@pool, statement, params, options)
  end

  def query!(statement, params \\ %{}, options \\ []) do
    Ch.query!(@pool, statement, params, options)
  end

  def to_maps(%{names: names, rows: rows}) do
    Enum.map(rows, fn row -> names |> Enum.zip(row) |> Map.new() end)
  end

  def setup_toxiproxy_counter do
    if :persistent_term.get(@toxiproxy_counter, nil) do
      raise "toxiproxy counter is already set up"
    end

    :persistent_term.put(@toxiproxy_counter, :atomics.new(1, signed: false))
  end

  def create_toxiproxy(upstream) do
    index =
      @toxiproxy_counter
      |> :persistent_term.get()
      |> :atomics.add_get(1, 1)

    name = "ch_test-#{index}"
    port = 8474 + index

    toxiproxy("POST", "/proxies", %{
      "name" => name,
      "listen" => "0.0.0.0:#{port}",
      "upstream" => upstream,
      "enabled" => true
    })

    ExUnit.Callbacks.on_exit(fn -> toxiproxy("DELETE", "/proxies/#{name}") end)

    %{name: name, url: "http://localhost:#{port}"}
  end

  def toxiproxy_up(%{name: name}) do
    toxiproxy("POST", "/proxies/#{name}", %{"enabled" => true})
  end

  def toxiproxy_down(%{name: name}) do
    toxiproxy("POST", "/proxies/#{name}", %{"enabled" => false})
  end

  def add_toxic(%{name: name}, %{"name" => _toxic_name} = toxic) do
    toxiproxy("POST", "/proxies/#{name}/toxics", toxic)
  end

  def remove_toxic(%{name: name}, toxic_name) do
    toxiproxy("DELETE", "/proxies/#{name}/toxics/#{toxic_name}")
  end

  def toxiproxy(method, path, data \\ nil) do
    headers = if data, do: [{"content-type", "application/json"}], else: []
    body = if data, do: JSON.encode_to_iodata!(data), else: nil

    case http(method, "http://localhost:8474#{path}", headers, body) do
      {:ok, status, _headers, _response_body} when status in 200..299 ->
        :ok

      {:ok, status, _headers, response_body} ->
        raise "unexpected status #{status} from Toxiproxy: #{response_body}"

      {:error, reason} ->
        raise reason
    end
  end

  def http(method, url, headers \\ [], body \\ nil) do
    uri = URI.parse(url)
    path = if uri.query, do: "#{uri.path}?#{uri.query}", else: uri.path

    case Mint.HTTP.connect(:http, uri.host, uri.port, mode: :passive) do
      {:ok, conn} ->
        case Mint.HTTP.request(conn, method, path, headers, body || "") do
          {:ok, conn, request_ref} ->
            receive_http_response(conn, request_ref, nil, [], [])

          {:error, conn, reason} ->
            Mint.HTTP.close(conn)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp receive_http_response(conn, request_ref, status, headers, body) do
    case Mint.HTTP.recv(conn, 0, to_timeout(second: 5)) do
      {:ok, conn, responses} ->
        case reduce_http_responses(responses, request_ref, status, headers, body) do
          {:done, status, headers, body} ->
            Mint.HTTP.close(conn)
            {:ok, status, headers, IO.iodata_to_binary(body)}

          {:more, status, headers, body} ->
            receive_http_response(conn, request_ref, status, headers, body)
        end

      {:error, conn, reason, _responses} ->
        Mint.HTTP.close(conn)
        {:error, reason}
    end
  end

  defp reduce_http_responses(responses, request_ref, status, headers, body) do
    Enum.reduce_while(responses, {:more, status, headers, body}, fn
      {:status, ^request_ref, status}, {:more, _status, headers, body} ->
        {:cont, {:more, status, headers, body}}

      {:headers, ^request_ref, new_headers}, {:more, status, headers, body} ->
        {:cont, {:more, status, headers ++ new_headers, body}}

      {:data, ^request_ref, data}, {:more, status, headers, body} ->
        {:cont, {:more, status, headers, [body, data]}}

      {:done, ^request_ref}, {:more, status, headers, body} ->
        {:halt, {:done, status, headers, body}}

      _response, acc ->
        {:cont, acc}
    end)
  end
end
