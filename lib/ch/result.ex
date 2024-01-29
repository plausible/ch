defmodule Ch.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select`, `:insert`;
    * `rows` - The result set. One of:
      - a list of lists, each inner list corresponding to a
        row, each element in the inner list corresponds to a column;
      - raw iodata when the response is not automatically decoded, e.g. `x-clickhouse-format: CSV`
    * `num_rows` - The number of fetched or affected rows;
    * `headers` - The HTTP response headers
  """

  defstruct [:command, :num_rows, :rows, :headers]

  @type t :: %__MODULE__{
          command: Ch.Query.command(),
          num_rows: non_neg_integer | nil,
          rows: [[term]] | iodata | nil,
          headers: Mint.Types.headers()
        }
end
