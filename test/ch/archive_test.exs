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

  test "test" do
    File.rm("test.zip")
    File.touch("test.zip")
    fd = File.open!("test.zip", [:binary, :raw, :append])

    {events_entry, encoded} = Dev.make_entry("events_v2.csv", "1,2")
    :ok = :file.write(fd, encoded)

    {sessions_entry, encoded} = Dev.make_entry("sessions_v2.csv", "3,4")
    :ok = :file.write(fd, encoded)

    encoded = Dev.encode_central_directory([events_entry, sessions_entry])
    :ok = :file.write(fd, encoded)

    :ok = File.close(fd)

    assert {:ok, contents} = :zip.table(File.read!("test.zip"))

    assert [
             zip_comment(comment: ~c"Created by Plausible"),
             zip_file(
               name: ~c"sessions_v2.csv",
               comment: ~c"",
               offset: 0,
               comp_size: 3,
               info: file_info(size: 3, type: :regular, access: :read_write, mode: 0o66)
             ),
             zip_file(
               name: ~c"events_v2.csv",
               comment: ~c"",
               offset: 64,
               comp_size: 3,
               info: file_info(size: 3, type: :regular, access: :read_write, mode: 0o66)
             )
           ] = contents

    assert :zip.unzip(~c"test.zip") == []
  end
end
