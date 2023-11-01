defmodule Ch.ArchiveTest do
  use ExUnit.Case, async: true

  require Record

  for {header, records} <- %{
        "kernel/include/file.hrl" => [:file_info],
        "stdlib/include/zip.hrl" => [:zip_file, :zip_comment]
      } do
    for record <- records do
      Record.defrecordp(record, Record.extract(record, from_lib: header))
    end
  end

  def export(conn, queries, on_data, opts \\ []) do
    entries =
      Enum.map(queries, fn {name, sql, params} ->
        Ch.run(conn, fn conn ->
          packets = Ch.stream(conn, sql, params, opts)
          {entry, encoded} = Dev.zip_start_entry(name)
          :ok = on_data.(encoded)

          entry =
            Enum.reduce(packets, entry, fn packets, entry ->
              Enum.reduce(packets, entry, fn packet, entry ->
                case packet do
                  {:data, _ref, data} ->
                    :ok = on_data.(data)
                    Dev.zip_grow_entry(entry, data)

                  _other ->
                    entry
                end
              end)
            end)

          {entry, encoded} = Dev.zip_end_entry(entry)
          :ok = on_data.(encoded)
          entry
        end)
      end)

    :ok = on_data.(Dev.zip_encode_central_directory(entries))
  end

  test "test" do
    File.rm("test.zip")
    File.touch("test.zip")
    fd = File.open!("test.zip", [:binary, :raw, :append])

    {:ok, conn} = Ch.start_link()

    sql =
      "select * from generateRandom('a String, b Int64, c UInt64') limit {limit:UInt32} format CSVWithNames"

    # opts = [settings: [enable_http_compression: 1], headers: [{"accept-encoding", "zstd"}]]

    :ok =
      export(
        conn,
        _queries = [
          {"events_v2.csv", sql, %{"limit" => 100_000}},
          {"sessions_v2.csv", sql, %{"limit" => 10000}}
        ],
        _on_data = fn data -> :file.write(fd, data) end
      )

    :ok = File.close(fd)

    assert {:ok, contents} = :zip.table(File.read!("test.zip"))

    assert [
             zip_comment(comment: ~c""),
             zip_file(
               name: ~c"events_v2.csv",
               comment: ~c""
               #  offset: 0,
               #  comp_size: 3,
               #  info: file_info(size: 3, type: :regular, access: :read_write, mode: 0o66)
             ),
             zip_file(
               name: ~c"sessions_v2.csv",
               comment: ~c""
               #  offset: 62,
               #  comp_size: 3,
               #  info: file_info(size: 3, type: :regular, access: :read_write, mode: 0o66)
             )
           ] = contents

    assert :zip.unzip(~c"test.zip") == {:ok, [~c"events_v2.csv", ~c"sessions_v2.csv"]}
  end
end
