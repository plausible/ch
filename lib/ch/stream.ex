defmodule Ch.Stream do
  @moduledoc false

  @derive {Inspect, only: []}
  defstruct [:conn, :ref, :query, :params, :opts]

  @type t :: %__MODULE__{
          conn: DBConnection.conn(),
          ref: Mint.Types.request_ref() | nil,
          query: Ch.Query.t(),
          params: term,
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
      ref = DBConnection.execute!(conn, query, {:stream, params}, opts)
      {%{stream | ref: ref}, &collect/2}
    end

    defp collect(%{conn: conn, query: query, ref: ref} = stream, {:cont, data}) do
      ^ref = DBConnection.execute!(conn, query, {:stream, ref, data})
      stream
    end

    defp collect(%{conn: conn, query: query, ref: ref}, eof) when eof in [:halt, :done] do
      DBConnection.execute!(conn, query, {:stream, ref, :eof})
    end
  end
end
