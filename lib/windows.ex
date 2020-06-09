defmodule Windows do
  @moduledoc """
  Showing how Flow.Window can be useful.
  """

  @doc """
  Returns coordinates (as tuples) of all unique stations in file "pollution.csv" without using Flow.Window which
  sometimes makes function working improperly.".
  """
  def get_stations do
    stream = File.stream!("lib/pollution/pollution.csv")

    stations_coords = stream
                      |> Flow.from_enumerable()
                      |> Flow.flat_map(&(String.split(&1, ",")))
                      |> Flow.map(&Float.parse/1)
                      |> Flow.filter(&(elem(&1, 1) == ""))
                      |> Flow.map(fn {x, _} -> x end)
                      |> Enum.to_list()

    first_coordinate = Enum.take_every(stations_coords, 2)
    second_coordinate = Enum.drop_every(stations_coords, 2)

    stations = Enum.zip(first_coordinate, second_coordinate)
               |> Enum.uniq()

    stations
  end

  @doc """
  Returns coordinates (as tuples) of all unique stations in file "pollution.csv" using Flow.Window.
  """
  def get_stations_window do
    stream = File.stream!("lib/pollution/pollution.csv")

    window = Flow.Window.count(1)

    stations_coords = stream
                      |> Flow.from_enumerable()
                      |> Flow.partition(window: window, stages: 1)
                      |> Flow.flat_map(&(String.split(&1, ",")))
                      |> Flow.map(&Float.parse/1)
                      |> Flow.filter(&(elem(&1, 1) == ""))
                      |> Flow.map(fn {x, _} -> x end)
                      |> Enum.to_list()

    first_coordinate = Enum.take_every(stations_coords, 2)
    second_coordinate = Enum.drop_every(stations_coords, 2)

    stations = Enum.zip(first_coordinate, second_coordinate)
               |> Enum.uniq()

    stations
  end

  @doc """
  Tests whether function `get_stations` returns the same result in two consecutive attempts.

  Returns boolean value - result of comparison.
  """
  def test do
    stations_1 = Enum.sort get_stations()
    stations_2 = Enum.sort get_stations()

    stations_1 == stations_2
  end

  @doc """
  Tests whether function `get_stations_window` returns the same result in two consecutive attempts.

  Returns boolean value - result of comparison.
  """
  def test_window do
    stations_1 = Enum.sort get_stations_window()
    stations_2 = Enum.sort get_stations_window()

    stations_1 == stations_2
  end
end
