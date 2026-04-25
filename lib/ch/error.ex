defmodule Ch.Error do
  @moduledoc "Error struct wrapping ClickHouse error responses."
  defexception [:code, :message]

  @typedoc """
  The Error struct. See [ErrorCodes.cpp](https://github.com/ClickHouse/ClickHouse/blob/5ce532e6f930c6f7fbdfa98b0327cc007df894b7/src/Common/ErrorCodes.cpp#) for possible errors.

  ## Fields

    * `:code` - The ClickHouse numeric error code
    * `:message` - The error message returned by the server
  """
  @type t :: %__MODULE__{
          code: non_neg_integer | nil,
          message: String.t()
        }
end
