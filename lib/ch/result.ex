defmodule Ch.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command, for example: `:select`, `:insert`;
    * `rows` - A list of lists, each inner list corresponding to a row, each element in the inner list corresponds to a column
    * `num_rows` - The number of fetched or affected rows;
    * `headers` - The HTTP response headers
    * `data` - The raw iodata from the response
  """

  defstruct [:command, :num_rows, :rows, :headers, :data]

  @type t :: %__MODULE__{
          command: Ch.Query.command(),
          num_rows: non_neg_integer | nil,
          rows: [[term]] | iodata | nil,
          headers: Mint.Types.headers(),
          data: iodata
        }
end
