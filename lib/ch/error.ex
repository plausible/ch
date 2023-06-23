defmodule Ch.Error do
  @moduledoc "Error struct wrapping ClickHouse error responses."
  defexception [:code, :message]
  @type t :: %__MODULE__{code: pos_integer | nil, message: String.t()}
end
