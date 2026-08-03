defmodule Ch.RowBinary.Plan do
  @moduledoc false

  @opaque t :: %__MODULE__{encoding_types: [term()], decoding_types: [term()]}

  @enforce_keys [:encoding_types, :decoding_types]
  defstruct [:encoding_types, :decoding_types]
end
