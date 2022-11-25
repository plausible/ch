defmodule Ch.Protocol do
  @moduledoc false
  use DBConnection
  alias Ch.Error

  @impl true
  def connect(opts) do
    scheme = String.to_existing_atom(opts[:scheme] || "http")
    address = opts[:host] || "localhost"
    port = opts[:port] || 8123
    Mint.HTTP1.connect(scheme, address, port)
  end

  @impl true
  def checkout(conn) do
    {:ok, conn}
  end

  @impl true
  def handle_begin(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_rollback(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_status(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_prepare(query, _opts, conn) do
    {:ok, query, conn}
  end

  @impl true
  def handle_execute(query, _params, _opts, conn) do
    # TODO
    {:ok, query, _result = nil, conn}
  end

  @impl true
  def handle_close(_query, _opts, conn) do
    {:ok, _result = nil, conn}
  end

  @impl true
  def handle_declare(_query, _params, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def disconnect(_error, conn) do
    Mint.HTTP1.close(conn)
    :ok
  end
end
