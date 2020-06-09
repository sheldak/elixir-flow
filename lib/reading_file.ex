defmodule ReadingFile do
  @moduledoc false

  def make_histogram_enum() do
    {:ok, file} = File.open("resources/histogram_enum.txt", [:write])

    File.read!("resources/alice.txt")
    |> String.split("\r\n")
    |> Enum.flat_map(&String.split(&1, " "))
    |> Enum.map(&String.trim(&1))
    |> Enum.filter(fn word -> String.length(word) > 0 end)
    |> Enum.reduce(%{}, fn word, acc -> Map.update(acc, word, 1, & &1 + 1) end)
    |> Enum.sort()
    |> Enum.map(fn {word, num} -> "#{word} : #{num}\n" end)
    |> Enum.each(fn text -> IO.write(file, text) end)

    File.close file

    :ok
  end

  def make_histogram_stream() do
    {:ok, file} = File.open("resources/histogram_stream.txt", [:write])

    File.stream!("resources/alice.txt")
    |> Stream.flat_map(&String.split(&1, " "))
    |> Stream.map(&String.trim(&1))
    |> Stream.filter(fn word -> String.length(word) > 0 end)
    |> Enum.reduce(%{}, fn word, acc -> Map.update(acc, word, 1, & &1 + 1) end)
    |> Enum.sort()
    |> Stream.map(fn {word, num} -> "#{word} : #{num}\n" end)
    |> Enum.each(fn text -> IO.write(file, text) end)

    File.close file

    :ok
  end

  def make_histogram_flow() do
    {:ok, file} = File.open("resources/histogram_flow.txt", [:write])

    File.stream!("resources/alice.txt")
    |> Flow.from_enumerable()
    |> Flow.flat_map(&String.split(&1, " "))
    |> Flow.map(&String.trim(&1))
    |> Flow.filter(fn word -> String.length(word) > 0 end)
    |> Flow.partition()
    |> Flow.reduce(fn -> %{} end, fn word, acc -> Map.update(acc, word, 1, & &1 + 1) end)
    |> Flow.map(fn {word, num} -> "#{word} : #{num}\n" end)
    |> Enum.to_list()
    |> Enum.sort()
    |> Enum.each(fn text -> IO.write(file, text) end)

    File.close file

    :ok
  end

  def compare_files(path_1, path_2) do
    case File.read(path_1) == File.read(path_2) do
      true ->  IO.puts "Contents of files are the same!"
      false -> IO.puts "Contents of files are different"
    end
  end

  def test do
    enum_time = fn -> make_histogram_enum() end
                |> :timer.tc
                |> elem(0)
                |> Kernel./(1_000_000)

    stream_time = fn -> make_histogram_stream() end
                |> :timer.tc
                |> elem(0)
                |> Kernel./(1_000_000)

    flow_time = fn -> make_histogram_flow() end
                |> :timer.tc
                |> elem(0)
                |> Kernel./(1_000_000)

    IO.puts "Enum time:   #{enum_time}s"
    IO.puts "Stream time: #{stream_time}s"
    IO.puts "Flow time:   #{flow_time}s"

    compare_files("resources/histogram_enum.txt", "resources/histogram_flow.txt")
    compare_files("resources/histogram_stream.txt", "resources/histogram_flow.txt")

    :ok
  end
end
