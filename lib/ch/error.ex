defmodule Ch.Error do
  @moduledoc "Error struct wrapping ClickHouse error responses."
  defexception [:code, :message]

  @typedoc """
  The Error struct.

  ## Fields

    * `:code` - The ClickHouse numeric error code
    * `:message` - The error message returned by the server
  """
  @type t :: %__MODULE__{code: pos_integer | nil, message: String.t()}
end
