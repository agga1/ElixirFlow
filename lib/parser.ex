defmodule Parser do
  @moduledoc """
mix compile
iex -S mix
    then run functions
"""
  def parseLine(line) do
    [date, time, lng, lat, value] = String.split(line, ",")
    %{:datetime =>
      {
        date
          |> String.split("-")
          |> Enum.map(&String.to_integer/1)
          |> Enum.reverse
          |> List.to_tuple,
        time
          |> String.split(":")
          |> Enum.map(&String.to_integer/1)
          |> List.insert_at(2, 0)
          |> List.to_tuple
    },
      :type => 'PM10',
      :location => {lng |> Float.parse |> elem(0),
                    lat |> Float.parse |> elem(0)},
      :pollutionLevel => value |> Integer.parse |> elem(0)
    }
  end

  def measure(func) do
    func  |> :timer.tc
          |> elem(0)
          |> Kernel./(1_000_000)
    end

  def loadStations(measurements) do
        measurements
        |> Enum.map(fn %{:location => location} -> location end)
        |> Enum.uniq
        |> Enum.each(&addStation/1)
  end

  def loadMeasurements(measurements) do
        measurements |> Enum.each(&addMeasurement/1)
  end

  def getMeasurements(path) do
    path
      |> File.read!
      |> String.split("\r\n")
      |> Enum.map(&parseLine/1)
  end

  def parse(path) do
    {prep_time, measurements} = fn -> getMeasurements path end |> :timer.tc

    :pollution_sup.start_link()
    add_stations_time =     measure(fn -> loadStations(measurements) end)
    add_measurements_time = measure(fn -> loadMeasurements(measurements) end)

    IO.puts "Adding times:"
    IO.puts "stations: #{add_stations_time+prep_time/(1_000_000)}"
    IO.puts "measurements: #{add_measurements_time}"

  end

  def example() do
    station = {20.06, 49.986}
    day = {2017, 5, 3}

    {sm_time, station_mean} = fn -> :pollution_gen_server.getStationMean(station, 'PM10') end
                              |> :timer.tc

    {dm_time, daily_mean} = fn -> :pollution_gen_server.getDailyMean(day, 'PM10') end
                              |> :timer.tc
    IO.puts "Station mean #{station_mean}\n\ttime: #{sm_time |> Kernel./(1_000_000)}"
    IO.puts "Daily mean #{daily_mean}\n\ttime: #{dm_time |> Kernel./(1_000_000)}"

  end

  def addStation({lng, lat}) do
    :pollution_gen_server.addStation( 'station_#{lng}_#{lat}', {lng, lat})
    end

  def addMeasurement(%{:datetime => datetime, :location => location, :type => type, :pollutionLevel => value}) do
    :pollution_gen_server.addValue(location, datetime, type, value)
  end

end