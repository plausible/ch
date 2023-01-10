defmodule Ch.Query do
  @moduledoc false
  defstruct [:statement, :command]

  def build(statement, opts) when is_list(opts) do
    build(statement, opts[:command] || extract_command(statement))
  end

  def build(statement, command) when is_atom(command) do
    %__MODULE__{statement: statement, command: command}
  end

  # TODO add iolist support [" i" | [["NSE", "RT"]]], etc.
  def extract_command("INSERT" <> _rest), do: :insert
  def extract_command("insert" <> _rest), do: :insert
  def extract_command(_other), do: nil

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query
    def describe(query, _opts), do: query
    def encode(_query, params, _opts), do: params
    def decode(_query, result, _opts), do: result
  end

  defimpl String.Chars do
    def to_string(%{statement: statement}) do
      IO.iodata_to_binary(statement)
    end
  end
end
