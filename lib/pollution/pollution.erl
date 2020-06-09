%%%-------------------------------------------------------------------
%% @doc pollution functions.
%% @end
%%%-------------------------------------------------------------------

-module(pollution).
-author("Samuel Heldak").

-include_lib("eunit/include/eunit.hrl").

%% API
-export([createMonitor/0, addStation/3, addValue/5, removeValue/4, getOneValue/4, getStationMean/3, getDailyMean/3,
         getStationWithTheMostMeasurements/1, getNearestStation/2]).


%%                                     ----- Structure of the Monitor -----
%%
%% Monitor is a map (#{}). Key is a pair of the coordinates (tuple: {X, Y}) of the station.
%% Value (which represents station) is an another map.
%% Every value always contain name of the station (name => "here is the name") and zero or more measurements.
%% Measurement has key and value. Key for a measurement is its date nad type. Value is the measurement.
%% For pollution (types: "PM10", "PM2.5") it is a number expressed in ug/m^3.
%% For temperature (type: "temp") it is a number of degrees Celsius.
%% Measurement example: {{{2020, 04, 02}, {12, 16, 44}}, "PM10"} => 15.
%% Station example: {1,1} => #{name => "Krakow",
%%                            {{{2020, 04, 02}, {12, 00, 00}}, "PM10"} => 150,
%%                            {{{2020, 04, 02}, {12, 00, 00}}, "PM2.5"} => 60,
%%                            {{{2020, 04, 02}, {12, 00, 00}}, "temp"} => 12}.


%% creating new monitor
createMonitor() -> #{}.


%% auxiliary function for getting length of the list
listLen([]) -> 0;
listLen([_ | T]) -> 1 + listLen(T).


%% function checking if there is a station with name equal to Name in the Monitor
isStation(Name, Monitor) ->
  Pred = fun(M) -> listLen(lists:filter(fun(V) -> V == Name end, maps:values(M))) == 1 end,
  ListWithStation = lists:filter(Pred, maps:values(Monitor)),

  case listLen(ListWithStation) of
    0 -> false;
    _ -> true
  end.


%% function adding station if there is no other station with the same Name or coordinates ({X, Y})
%% in case of duplication function creates error
addStation(Name, {X, Y}, Monitor) ->
  case maps:is_key({X, Y}, Monitor) or isStation(Name, Monitor) of
    true -> {error, duplicated_station};
    _    -> Monitor#{{X, Y} => #{name => Name}}
  end.


%% auxiliary function for searching coordinates of the station ({X, Y}) when have just its Name
findKey(Name, Monitor) ->
  Pred = fun({_, M}) -> listLen(lists:filter(fun(V) -> V == Name end, maps:values(M))) == 1 end,
  [{K, _}] = lists:filter(Pred, maps:to_list(Monitor)),

  K.


%% adding new measurement to the station (by passing coordinates ({X, Y}) or Name)
%% in case of duplication of Date and Type or non-existence of the station, function creates error
addValue({X, Y}, Date, Type, Value, Monitor) ->
  case maps:is_key({X, Y}, Monitor) of
    true -> StationValues = maps:get({X, Y}, Monitor),

            case maps:is_key({Date, Type}, StationValues) of
              true -> {error, duplicated_value};
              _    -> Monitor#{{X, Y} => maps:put({Date, Type}, Value, StationValues)}
            end;
    _    -> {error, no_station}
  end;

addValue(Name, Date, Type, Value, Monitor) ->
  case isStation(Name, Monitor) of
    true -> addValue(findKey(Name, Monitor), Date, Type, Value, Monitor);
    _    -> {error, no_station}
  end.


%% removing a measurement (or doing nothing if such measurement does not exist)
%% in case of non-existence of station, function creates error
removeValue({X, Y}, Date, Type, Monitor) ->
  case maps:is_key({X, Y}, Monitor) of
    true -> StationValues = maps:get({X, Y}, Monitor),
            Monitor#{{X, Y} => maps:remove({Date, Type}, StationValues)};
    _    -> {error, no_station}
  end;

removeValue(Name, Date, Type, Monitor) ->
  case isStation(Name, Monitor) of
    true -> removeValue(findKey(Name, Monitor), Date, Type, Monitor);
    _    -> {error, no_station}
  end.


%% getting value of the measurement (Name or coordinates ({X, Y}) and Date and Type needed)
%% in case of non-existence of station or the measurement, function creates error
getOneValue({X, Y}, Date, Type, Monitor) ->
  case maps:is_key({X, Y}, Monitor) of
    true -> StationValues = maps:get({X, Y}, Monitor),

            case maps:is_key({Date, Type}, StationValues) of
              true -> maps:get({Date, Type}, StationValues);
              _    -> {error, no_value}
            end;
    _    -> {error, no_station}
  end;

getOneValue(Name, Date, Type, Monitor) ->
  case isStation(Name, Monitor) of
    true -> getOneValue(findKey(Name, Monitor), Date, Type, Monitor);
    _    -> {error, no_station}
  end.


%% getting mean value of all measurements at the station (given by Name or coordinates ({X, Y}) of given Type)
%% in case of non-existence of station, function creates error
%% in case of zero measurements, function returns 0
getStationMean({X, Y}, Type, Monitor) ->
  case maps:is_key({X, Y}, Monitor) of
    true -> StationValues = maps:get({X, Y}, Monitor),
            StationMeasurements = maps:filter(fun({_D, T}, _V) -> T == Type; (_, _) -> false end, StationValues),
            MeasurementsNumber = max(maps:size(StationMeasurements), 1),
            MeasurementsSum = maps:fold(fun(_K, V, Acc) -> V + Acc end, 0, StationMeasurements),
            MeasurementsSum / MeasurementsNumber;
    _    -> {error, no_station}
  end;

getStationMean(Name, Type, Monitor) ->
  case isStation(Name, Monitor) of
    true -> getStationMean(findKey(Name, Monitor), Type, Monitor);
    _    -> {error, no_station}
  end.


%% getting daily mean value of measurements of the given Type from all stations in the Monitor
%% in case of no measurements, function returns 0
getDailyMean(Day, Type, Monitor) ->
  DayTypeFilterPred = fun ({{D, _H}, T}, _V) -> X = T == Type,
                                                Y = D == Day,
                                                X and Y;
                            (_, _)           -> false
                      end,
  FilteredMonitor = maps:map(fun(_K, V) -> maps:filter(DayTypeFilterPred, V) end, Monitor),

  FoldStationFunc = fun(_K, V, Acc) -> Acc + V end,
  DailySum = maps:fold(fun(_K, V, Acc) -> Acc + maps:fold(FoldStationFunc, 0, V) end, 0, FilteredMonitor),
  DailySize = max(maps:fold(fun(_K, V, Acc) -> Acc + maps:size(V) end, 0, FilteredMonitor), 1),

  DailySum / DailySize.


%% getting coordinates ({X, Y}) of the station which has the highest number of measurements
%% in case of no stations, function creates error
getStationWithTheMostMeasurements(Monitor) ->
  case maps:size(Monitor) of
    0 -> {error, empty_monitor};
    _ -> ListOfPairs = lists:map(fun({K, V}) -> {K, maps:size(V)} end, maps:to_list(Monitor)),
         [{K, _V} | _T] = lists:sort(fun({_K1, V1}, {_K2, V2}) -> V1 >= V2 end, ListOfPairs),
         K
  end.


%% auxiliary function which returns coordinates ({X1, Y1} or {X2, Y2})
%% which are closer to the {X, Y} in Euclidean the Metric
getNearerCoords({X, Y}, {X1, Y1}, {X2, Y2}) ->
  case ((X-X1)*(X-X1) + (Y-Y1)*(Y-Y1)) =< ((X-X2)*(X-X2) + (Y-Y2)*(Y-Y2)) of
    true -> {X1, Y1};
    _    -> {X2, Y2}
  end.


%% function returning coordinates of the station which are the closest to the given one ({X, Y})
%% in case of non-existence of station with coordinates {X, Y}
%% or just one station in the Monitor, function creates error
getNearestStation({X, Y}, Monitor) ->
  case maps:is_key({X, Y}, Monitor) of
    true -> case maps:size(Monitor) of
              1 -> {error, just_one_station};
              _ -> OtherStations = maps:remove({X, Y}, Monitor),
                   [{StartKey, _V} | _T] = maps:to_list(OtherStations),
                   maps:fold(fun(Key, _, Best) -> getNearerCoords({X, Y}, Best, Key) end, StartKey, OtherStations)
            end;
    _    -> {error, no_station}
  end.