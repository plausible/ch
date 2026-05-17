defmodule Ch.Result do
  @moduledoc """
  ClickHouse query result.

  `Ch.query/4` returns this struct for successful responses.
  """

  defstruct [
    :names,
    :rows,
    :headers,
    :data
  ]

  @typedoc """
  Query result.

  ## Fields

    * `:names` - Column names returned by ClickHouse, or `nil` when Ch did not decode rows.
    * `:rows` - Decoded rows, or `nil` when Ch did not decode rows.
    * `:headers` - HTTP response headers.
    * `:data` - Raw response body iodata as received from ClickHouse.
  """
  @type t :: %__MODULE__{
          names: [String.t()] | nil,
          rows: [[term]] | nil,
          headers: Mint.Types.headers(),
          data: iodata | nil
        }
end
