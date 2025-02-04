defmodule Ch.Test do
  @moduledoc false

  def client_opts(overrides \\ []) do
    Application.fetch_env!(:ch, :default)
    |> Keyword.merge(overrides)
  end

  def database do
    Keyword.fetch!(client_opts(), :database)
  end

  # makes a query in a short lived process so that pool automatically exits once finished
  def sql_exec(sql, params \\ [], opts \\ []) do
    task =
      Task.async(fn ->
        {:ok, pid} = Ch.start_link(client_opts(opts))
        Ch.query!(pid, sql, params, opts)
      end)

    Task.await(task)
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
