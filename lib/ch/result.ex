defmodule Ch.Result do
  @moduledoc """
  Result struct returned from successful queries.
  """

  defstruct [:command, :num_rows, :columns, :rows, :headers, :data]

  @typedoc """
  The Result struct.
  """
  @type t :: %__MODULE__{
          command: atom | nil,
          num_rows: non_neg_integer | nil,
          columns: [String.t()] | nil,
          rows: [[term]] | iodata | nil,
          headers: Mint.Types.headers(),
          data: iodata
        }
end
