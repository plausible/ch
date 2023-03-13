defmodule Ch.Query do
  @moduledoc false
  @enforce_keys [:statement, :command]
  defstruct [:statement, :command]

  @type t :: %__MODULE__{statement: iodata, command: atom}

  @doc false
  @spec build(iodata, atom) :: t
  def build(statement, command \\ nil) when is_atom(command) do
    %__MODULE__{statement: statement, command: command || extract_command(statement)}
  end

  statements = [
    {"SELECT", :select},
    {"INSERT", :insert},
    {"CREATE", :create},
    {"ALTER", :alter},
    {"DELETE", :delete},
    {"SYSTEM", :system},
    {"SHOW", :show},
    # as of clickhouse 22.8, WITH is only allowed in SELECT
    # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
    {"WITH", :select},
    {"GRANT", :grant},
    {"EXPLAIN", :explain},
    {"REVOKE", :revoke},
    {"ATTACH", :attach},
    {"CHECK", :check},
    {"DESCRIBE", :describe},
    {"DETACH", :detach},
    {"DROP", :drop},
    {"EXISTS", :exists},
    {"KILL", :kill},
    {"OPTIMIZE", :optimize},
    {"RENAME", :rename},
    {"EXCHANGE", :exchange},
    {"SET", :set},
    {"TRUNCATE", :truncate},
    {"USE", :use},
    {"WATCH", :watch}
  ]

  @doc false
  def extract_command(statement)

  for {statement, command} <- statements do
    def extract_command(unquote(statement) <> _), do: unquote(command)
    def extract_command(unquote(String.downcase(statement)) <> _), do: unquote(command)
  end

  def extract_command(<<whitespace, rest::bytes>>) when whitespace in [?\s, ?\t, ?\n] do
    extract_command(rest)
  end

  # TODO cover more cases, don't rely on assumed format
  def extract_command([first | _]), do: extract_command(first)
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
