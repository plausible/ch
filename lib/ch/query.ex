defmodule Ch.Query do
  @moduledoc false
  defstruct [:statement, :command]

  def build(statement, opts \\ []) do
    _build(statement, opts[:command] || extract_command(statement))
  end

  defp _build(statement, command) do
    %__MODULE__{statement: statement, command: command}
  end

  # TODO add iolist support ["i" | [["NSE", "RT"]]], etc.
  defp extract_command("INSERT " <> _rest), do: :insert
  defp extract_command("insert " <> _rest), do: :insert
  defp extract_command(_other), do: nil

  # TODO since these are executed in the caller, maybe it's better to do encoding / decoding here?
  defimpl DBConnection.Query do
    def parse(query, _opts) do
      # IO.inspect([query: query, opts: opts, pid: self()], label: "Query.parse")
      query
    end

    def describe(query, _opts) do
      # IO.inspect([query: query, opts: opts, pid: self()], label: "Query.describe")
      query
    end

    def encode(_query, params, _opts) do
      # IO.inspect([query: query, params: params, opts: opts, pid: self()], label: "Query.encode")
      params
    end

    def decode(_query, result, _opts) do
      # IO.inspect([query: query, result: result, opts: opts, pid: self()], label: "Query.decode")
      result
    end
  end

  defimpl String.Chars do
    def to_string(%{statement: statement}) do
      IO.iodata_to_binary(statement)
    end
  end
end
