defmodule Ch.Stream do
  @moduledoc """
  Stream struct returned from stream commands.

  All of its fields are private.
  """

  @derive {Inspect, only: []}
  defstruct [:conn, :query, :params, :opts]

  @type t :: %__MODULE__{
          conn: DBConnection.conn(),
          query: Ch.Query.t(),
          params: Ch.params(),
          opts: [Ch.query_option()]
        }

  defimpl Enumerable do
    def reduce(stream, acc, fun) do
      %Ch.Stream{conn: conn, query: query, params: params, opts: opts} = stream
      stream = %DBConnection.Stream{conn: conn, query: query, params: params, opts: opts}
      DBConnection.reduce(stream, acc, fun)
    end

    def member?(_, _), do: {:error, __MODULE__}
    def count(_), do: {:error, __MODULE__}
    def slice(_), do: {:error, __MODULE__}
  end

  defimpl Collectable do
    def into(stream) do
      %Ch.Stream{conn: conn, query: query, params: params, opts: opts} = stream
      DBConnection.execute!(conn, query, params, opts)
      {stream, &collect/2}
    end

    defp collect(%{conn: conn, query: query} = stream, {:cont, data}) do
      DBConnection.execute!(conn, %{query | statement: data}, [])
      stream
    end

    defp collect(conn, :done), do: HTTP.stream_request_body(conn, ref(conn), :eof)
  end
end
