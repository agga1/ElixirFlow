defmodule ParserStream do
  @moduledoc false
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

  def loadStations(path) do
    path  |> File.stream!
          |> Stream.map(&(String.split(&1, ",")))
          |> Stream.map(fn [_, _, x, y, _] -> [x, y] end)
          |> Stream.map(fn [x, y] -> { x|> Float.parse |> elem(0),
                                       y|> Float.parse |> elem(0)} end)
          |> Stream.uniq()
          |> Enum.each(&addStation/1)
  end

  def loadMeasurements(path) do
    path  |> File.stream!
          |> Stream.map(&parseLine/1)
          |> Enum.each(&addMeasurement/1)
  end

  def parse(path) do
    :pollution_sup.start_link()
    add_stations_time =     measure(fn -> loadStations(path) end)
    add_measurements_time = measure(fn -> loadMeasurements(path) end)

    IO.puts "Adding times:"
    IO.puts "stations: #{add_stations_time}"
    IO.puts "measurements: #{add_measurements_time}"

  end

  def example() do
    station = {20.06, 49.986}
    day = {2017, 5, 3}

    {sm_time, station_mean} = fn -> :pollution_gen_server.getStationMean(station, 'PM10') end
                              |> :timer.tc

    {dm_time, daily_mean} = fn -> :pollution_gen_server.getDailyMean(day, 'PM10') end
                              |> :timer.tc
    IO.puts "Station mean #{station_mean}\n\ttime: #{sm_time}"
    IO.puts "Daily mean #{daily_mean}\n\ttime: #{dm_time}"

  end

  def addStation({lng, lat}) do
    :pollution_gen_server.addStation('station_#{lng}_#{lat}', {lng, lat})
    end

  def addMeasurement(%{:datetime => datetime, :location => location, :type => type, :pollutionLevel => value}) do
    :pollution_gen_server.addValue(location, datetime, type, value)
  end

end