defmodule Ch.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select`, `:insert`;
    * `rows` - The result set. One of:
      - a list of lists, each inner list corresponding to a
        row, each element in the inner list corresponds to a column;
      - raw iodata when the response is not automatically decoded, e.g. `x-clickhouse-format: CSV`
    * `num_rows` - The number of fetched or affected rows;
    * `meta` - A map of metadata collected from the response headers like `x-clickhouse-format`,
      `x-clickhouse-query-id`, `x-clickhouse-summary`, etc.
  """

  defstruct [:command, :meta, :num_rows, :rows]

  @type summary :: %{String.t() => String.t()}
  @type t :: %__MODULE__{
          command: atom,
          meta: %{String.t() => String.t() | summary},
          num_rows: non_neg_integer,
          rows: [[term]] | iodata | nil
        }
end
