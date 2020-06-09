defmodule PollutionDataEnum do
  @moduledoc """
  Loading data from file to the pollution server using Enum.
  """

  @doc """
  Gets lines from CSV file as list.

  Returns list.
  """
  def import_lines_from_CSV do
    lines = File.read!("lib/pollution/pollution.csv")
            |> String.split("\r\n")
    lines
  end

  @doc """
  Makes map containing information about 1 measurement from given `line`.

  Returns map with three items: `:datetime`, `:location`, `:pollution_level`.
  """
  def parse_line(line) do
    [date_str, time_str, x_str, y_str, value_str] = String.split(line, ",")

    date = date_str
           |> String.split("-")
           |> Enum.reverse()
           |> Enum.map(&Integer.parse/1)
           |> Enum.map(&(elem(&1, 0)))
           |> List.to_tuple()

    time = time_str
           |> String.split(":")
           |> Enum.map(&Integer.parse/1)
           |> Enum.map(&(elem(&1, 0)))
           |> List.to_tuple()

    datetime = {date, time}

    location = [x_str, y_str]
               |> Enum.map(&Float.parse/1)
               |> Enum.map(&(elem(&1, 0)))
               |> List.to_tuple()

    pollution_level = elem(Integer.parse(value_str), 0)

    %{:datetime => datetime, :location => location, :pollution_level => pollution_level}
  end

  @doc """
  Gets unique stations from `measurements` (list of maps).

  Returns list of unique locations of stations.
  """
  def identify_stations(measurements) do
    stations = measurements
               |> Enum.map(fn measurement -> measurement.location end)
               |> Enum.uniq()

    stations
  end

  @doc """
  Makes name of station from given `station_location`.

  Returns string which is a name of the station.
  """
  def generate_station_name(station_location) do
    "station_#{elem(station_location, 0)}_#{elem(station_location, 1)}"
  end

  @doc """
  Adds given `stations` (list of tuples representing locations) to the pollution server.
  """
  def add_stations(stations) do
    add_station_fn = fn station -> :pollution_gen_server.addStation(generate_station_name(station), station) end

    Enum.each(stations, add_station_fn)
  end

  @doc """
  Adds given `measurements` (list of maps) to the pollution server.
  """
  def add_measurements(measurements) do
    add_measurement_fn = fn measurement -> :pollution_gen_server.
                          addValue(measurement.location, measurement.datetime, "PM10", measurement.pollution_level) end

    Enum.each(measurements, add_measurement_fn)
  end

  @doc """
  Main function which gets list of lines from the file and saves all measurements to the pollution server.
  Function prints time needed to load stations and measurements.
  """
  def add_measurements_from_file do
    measurements = import_lines_from_CSV()
            |> Enum.map(&parse_line/1)
    stations = identify_stations(measurements)

    :pollution_sup.start_link()
    add_stations_time = fn -> add_stations(stations) end
                        |> :timer.tc
                        |> elem(0)
                        |> Kernel./(1_000_000)

    add_measurements_time = fn -> add_measurements(measurements) end
                            |> :timer.tc
                            |> elem(0)
                            |> Kernel./(1_000_000)

    :timer.sleep(200);
    IO.puts "Time of adding stations: #{add_stations_time}"
    IO.puts "Time of adding measurements: #{add_measurements_time}"
  end
end
