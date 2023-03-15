defmodule Ch.Health do
  @moduledoc """
  A helper health check process to minimise wait time in `ping/1` when in multihost mode.
  """

  use GenServer

  @typep state :: %{checking: [{Ch.Connection.endpoint(), reference}], config: Keyword.t()}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__, hibernate_after: :timer.minutes(30))
  end

  @spec is_alive(Ch.Connection.endpoint()) :: boolean
  def is_alive(endpoint) do
    case :ets.lookup(__MODULE__, endpoint) do
      [{_endpoint, _alive = true, timestamp}] ->
        recently_alive = check_age(timestamp) < :timer.minutes(1)
        unless recently_alive, do: start_check(endpoint)
        recently_alive

      [{_endpoint, not_alive = false, timestamp}] ->
        recently_checked = check_age(timestamp) < :timer.minutes(5)
        unless recently_checked, do: start_check(endpoint)
        not_alive

      [] ->
        start_check(endpoint)
        false
    end
  end

  defp start_check(endpoint) do
    GenServer.cast(__MODULE__, {:check, endpoint})
  end

  defp check_age(timestamp, now \\ now()) do
    now - timestamp
  end

  defp now, do: :os.system_time(:second)

  @impl true
  @spec init(Keyword.t()) :: {:ok, state}
  def init(opts) do
    _ensured_task_sup = Keyword.fetch!(opts, :task_sup)
    __MODULE__ = :ets.new(__MODULE__, [:named_table])
    {:ok, %{checking: [], config: opts}}
  end

  @impl true
  def handle_cast({:check, endpoint}, state) do
    %{checking: checking, config: config} = state
    already_checking? = List.keymember?(checking, endpoint, 0)

    if already_checking? do
      {:noreply, state}
    else
      %Task{ref: ref} = supervised_check(endpoint, config)
      checking = [{endpoint, ref} | checking]
      state = %{state | checking: checking}
      {:noreply, state}
    end
  end

  defp supervised_check(endpoint, config) do
    task_sup = Keyword.fetch!(config, :task_sup)

    Task.Supervisor.async_nolink(task_sup, fn ->
      case Ch.Connection.connect(endpoint, config) do
        {:ok, _conn} -> true
        {:error, _reason} -> false
      end
    end)
  end

  @impl true
  def handle_info({ref, alive}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, update_status(state, ref, alive)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, update_status(state, ref, false)}
  end

  defp update_status(state, ref, alive) do
    case List.keytake(state.checking, ref, 1) do
      {{endpoint, _ref}, checking} ->
        :ets.insert(__MODULE__, {endpoint, alive, now()})
        %{state | checking: checking}

      nil ->
        state
    end
  end
end
