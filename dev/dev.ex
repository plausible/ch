defmodule Dev do
  import Bitwise

  @comment "Created by Plausible"

  def encode_central_directory(entries) do
    context =
      Enum.reduce(entries, %{frames: [], count: 0, offset: 0, size: 0}, fn entry, acc ->
        header = encode_central_file_header(acc, entry)

        acc
        |> Map.update!(:frames, &[header.frame | &1])
        |> Map.update!(:count, &(&1 + 1))
        |> Map.update!(:offset, &(&1 + header.offset))
        |> Map.update!(:size, &(&1 + header.size))
      end)

    frame = <<
      0x06054B50::32-little,
      # number of this disk
      0::16,
      # number of the disk w/ ECD
      0::16,
      # total number of entries in this disk
      context.count::16-little,
      # total number of entries in the ECD
      context.count::16-little,
      # size central directory
      context.size::32-little,
      # offset central directory
      context.offset::32-little,
      # comment length
      byte_size(@comment)::16-little,
      @comment::bytes
    >>

    [:lists.reverse(context.frames), frame]
  end

  defp encode_central_file_header(context, %{header: header, entity: entity}) do
    mtime = NaiveDateTime.from_erl!(:calendar.local_time())

    frame = <<
      # central file header signature
      0x02014B50::32-little,
      # version made by
      52::16-little,
      # version to extract
      20::16-little,
      # general purpose bit flag (bit 3: data descriptor, bit 11: utf8 name)
      0x0008 ||| 0x0800::16-little,
      # compression method
      0::16-little,
      # last mod file time
      dos_time(mtime)::16-little,
      # last mod date
      dos_date(mtime)::16-little,
      # crc-32
      entity.crc::32-little,
      # compressed size
      entity.csize::32-little,
      # uncompressed size
      entity.usize::32-little,
      # file name length
      header.nsize::16-little,
      # extra field length
      0::16,
      # file comment length
      0::16,
      # disk number start
      0::16,
      # internal file attribute
      0::16,
      # external file attribute (unix permissions, rw-r--r--)
      (0o10 <<< 12 ||| 0o644) <<< 16::32-little,
      # relative offset header
      context.offset::32-little,
      # file name
      header.name::bytes
    >>

    %{frame: frame, size: byte_size(frame), offset: header.size + entity.size}
  end

  def make_entry(name, data) do
    mtime = NaiveDateTime.from_erl!(:calendar.local_time())
    crc = :erlang.crc32(data)

    # see 4.4 in https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
    local_header = <<
      # local file header signature
      0x04034B50::32-little,
      # version needed to extract
      20::16-little,
      # general purpose bit flag (bit 3: data descriptor, bit 11: utf8 name)
      0x0008 ||| 0x0800::16-little,
      # compression method (always 0, we aren't compressing currently)
      # TODO zstd = 93
      0::16-little,
      # last mod time
      dos_time(mtime)::16-little,
      # last mod date
      dos_date(mtime)::16-little,
      # crc-32
      0::32,
      # compressed size
      0::32,
      # uncompressed size
      0::32,
      # file name length
      byte_size(name)::16-little,
      # extra field length
      0::16,
      # file name
      name::bytes
    >>

    data_descriptor = <<
      # local file entry signature
      0x08074B50::32-little,
      # crc-32 for the entity
      crc::32-little,
      # compressed size, just the size since we aren't compressing
      byte_size(data)::32-little,
      # TODO
      # uncompressed size
      byte_size(data)::32-little
    >>

    encoded = [local_header, data, data_descriptor]

    entry = %{
      header: %{
        size: byte_size(local_header),
        name: name,
        nsize: byte_size(name)
      },
      entity: %{
        crc: crc,
        size: byte_size(data) + byte_size(data_descriptor),
        usize: byte_size(data),
        csize: byte_size(data)
      },
      size: IO.iodata_length(encoded)
    }

    {entry, encoded}
  end

  def dos_time(time) do
    round(time.second / 2 + (time.minute <<< 5) + (time.hour <<< 11))
  end

  def dos_date(time) do
    round(time.day + (time.month <<< 5) + ((time.year - 1980) <<< 9))
  end

  # def archive do
  #   spawn_link(fn ->
  #     {:ok, conn} = Ch.start_link()

  #     tmp_filepath =
  #       Path.join(
  #         System.tmp_dir!(),
  #         "ch_nums_#{System.os_time(:second)}_#{System.unique_integer([:positive])}.zst"
  #       )

  #     File.touch!(tmp_filepath)
  #     fd = File.open!(tmp_filepath, [:raw, :binary, :append])

  #     Ch.run(conn, fn conn ->
  #       conn
  #       |> Ch.stream("select * from system.numbers limit 10000000", [])
  #       |> Stream.scan(fn packets ->
  #         Enum.each(packets, fn packet ->
  #           with {:data, _ref, data} <- packet do
  #             :ok = :file.write(fd, data)
  #           end
  #         end)
  #       end)
  #       |> Stream.run()
  #     end)

  #     :ok = File.close(fd)
  #     IO.puts(tmp_filepath)
  #   end)
  # end

  # def export(queries, callback) do
  #   mtime = NaiveDateTime.from_erl!(:calendar.local_time())
  #   mod_time = dos_time(mtime)
  #   mod_date = dos_date(mtime)

  #   ch_opts = [
  #     format: "CSVWithNames",
  #     settings: [enable_http_compression: 1],
  #     headers: [{"accept-encoding", "zstd"}]
  #   ]

  #   Enum.reduce(queries, [], fn {query_name, sql, params} ->
  #     filename = query_name <> ".zst"

  #     local_header = <<
  #       # local file header signature
  #       0x04034B50::32-little,
  #       # version needed to extract
  #       20::16-little,
  #       # general purpose bit flag (bit 3: data descriptor, bit 11: utf8 name)
  #       0x0008 ||| 0x0800::16-little,
  #       # compression method (always 0, we aren't compressing currently)
  #       # TODO zstd = 93
  #       0::16-little,
  #       # last mod time
  #       mod_time::16-little,
  #       # last mod date
  #       mod_date::16-little,
  #       # crc-32
  #       0::32,
  #       # compressed size
  #       0::32,
  #       # uncompressed size
  #       0::32,
  #       # file name length
  #       byte_size(name)::16-little,
  #       # extra field length
  #       0::16,
  #       # file name
  #       name::bytes
  #     >>

  #     callback.(local_header)

  #     size =
  #       Ch.run(conn, fn conn ->
  #         conn
  #         |> Ch.stream(query, params, ch_opts)
  #         |> Enum.reduce(_size = 0, fn packets, acc_size ->
  #           nil
  #         end)
  #       end)
  #   end)
  # end
end
