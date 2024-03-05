defmodule Ch.HTTPTest do
  use ExUnit.Case, async: true

  @moduletag :slow

  describe "user-agent" do
    setup do
      {:ok, ch: start_supervised!(Ch)}
    end

    test "sets user-agent to ch/<version> by default", %{ch: ch} do
      %Ch.Result{rows: [[123]], headers: resp_header} = Ch.query!(ch, "select 123")
      {"x-clickhouse-query-id", query_id} = List.keyfind!(resp_header, "x-clickhouse-query-id", 0)
      assert query_http_user_agent(ch, query_id) == "ch/" <> Mix.Project.config()[:version]
    end

    test "uses the provided user-agent", %{ch: ch} do
      req_headers = [{"user-agent", "plausible/0.1.0"}]

      %Ch.Result{rows: [[123]], headers: resp_header} =
        Ch.query!(ch, "select 123", _params = [], headers: req_headers)

      {"x-clickhouse-query-id", query_id} = List.keyfind!(resp_header, "x-clickhouse-query-id", 0)
      assert query_http_user_agent(ch, query_id) == "plausible/0.1.0"
    end
  end

  defp query_http_user_agent(ch, query_id) do
    retry(fn ->
      %Ch.Result{rows: [[user_agent]]} =
        Ch.query!(
          ch,
          "select http_user_agent from system.query_log where query_id = {query_id:String} limit 1",
          %{"query_id" => query_id}
        )

      user_agent
    end)
  end

  defp retry(f) do
    try do
      f.()
    catch
      _, _ ->
        :timer.sleep(100)
        retry(f)
    end
  end
end
