%%%-------------------------------------------------------------------
%% @doc pollution generic server.
%% @end
%%%-------------------------------------------------------------------

-module(pollution_gen_server).
-author("Samuel Heldak").

-behavior(gen_server).

%% API
-export([start/1, init/1, stop/0, handle_call/3, handle_cast/2, terminate/2]).
-export([crash/0, addStation/2, addValue/4, removeValue/3, getOneValue/3, getStationMean/2, getDailyMean/2,
  getStationWithTheMostMeasurements/0, getNearestStation/1]).


%% ----- USER INTERFACE -----

start(InitialMonitor) ->
  gen_server:start_link({local, pollution}, ?MODULE, InitialMonitor, []).


init(InitialMonitor) ->
  io:format("~n=================  Server init  ==================~n~n"),
  {ok, InitialMonitor}.


stop() ->
  gen_server:cast(pollution, {stop}).


% for crash test
crash() ->
  gen_server:cast(pollution, {crash}).


%% adding new station
addStation(Name, Coordinates) ->
  gen_server:cast(pollution, {addStation, Name, Coordinates}).


%% adding new measurement to the station
addValue(Name, Date, Type, Value) ->
  gen_server:cast(pollution, {addValue, Name, Date, Type, Value}).


%% removing a measurement
removeValue(Name, Date, Type) ->
  gen_server:cast(pollution, {removeValue, Name, Date, Type}).


%% getting value of the measurement
getOneValue(Name, Date, Type) ->
  gen_server:call(pollution, {getOneValue, Name, Date, Type}).


%% getting mean value of all the measurements at the station
getStationMean(Name, Type) ->
  gen_server:call(pollution, {getStationMean, Name, Type}).


%% getting daily mean value of measurements of the given Type from all stations
getDailyMean(Day, Type) ->
  gen_server:call(pollution, {getDailyMean, Day, Type}).


%% getting coordinates ({X, Y}) of the station which has the highest number of measurements
getStationWithTheMostMeasurements() ->
  gen_server:call(pollution, {getStationWithTheMostMeasurements}).


%% function returning coordinates of the station which are the closest to the given one
getNearestStation(Coordinates) ->
  gen_server:call(pollution, {getNearestStation, Coordinates}).



%% ----- AUXILIARY FUNCTIONS -----
printError(ErrorName,  {ErrorName1, ErrorMsg1, ErrorMessage1Args}, {ErrorName2, ErrorMsg2, ErrorMessage2Args}) ->
  case ErrorName of
    ErrorName1 ->
      io:format(ErrorMsg1, ErrorMessage1Args);
    ErrorName2 ->
      io:format(ErrorMsg2, ErrorMessage2Args)
  end.

handleCast({error, ErrorName}, Error1, Error2, _Success, Monitor) ->
  printError(ErrorName, Error1, Error2),
  {noreply, Monitor};

handleCast(NewMonitor, _Error1, _Error2, {Message, MessageArguments}, _Monitor) ->
  io:format(Message, MessageArguments),
  {noreply, NewMonitor}.


handleCall({error, ErrorName}, Error1, Error2, _Success, Monitor) ->
  printError(ErrorName, Error1, Error2),
  {reply, {error, ErrorName}, Monitor};

handleCall(Result, _Error1, _Error2, {Message, MessageArguments}, Monitor) ->
  io:format(Message, MessageArguments),
  {reply, Result, Monitor}.



%% ----- CALLBACKS -----
handle_cast({crash}, Monitor) ->
  pollution:function_that_not_exist(),
  {noreply, Monitor};

handle_cast({stop}, Monitor) ->
  {stop, terminated, Monitor};

handle_cast({addStation, Name, Coordinates}, Monitor) ->
  Result = pollution:addStation(Name, Coordinates, Monitor),
  handleCast(Result,
    {duplicated_station, "Cannot add station. Duplicated station's name or coordinates.~n", []}, {[], [], []},
    {"Station ~s added.~n", [Name]}, Monitor);

handle_cast({addValue, Name, Date, Type, Value}, Monitor) ->
  Result = pollution:addValue(Name, Date, Type, Value, Monitor),
  handleCast(Result,
    {no_station, "Cannot add value. Cannot find station with that name.~n", []},
    {duplicated_value, "Cannot add value. Duplicated measurement.~n", []},
    {"Measurement ~s = ~B added to the station.~n", [Type, Value]}, Monitor);

handle_cast({removeValue, Name, Date, Type}, Monitor) ->
  Result = pollution:removeValue(Name, Date, Type, Monitor),
  handleCast(Result,
    {no_station, "Cannot remove value. Cannot find station with that name.~n", []}, {[], [], []},
    {"Measurement removed from the station.~n", []}, Monitor).


handle_call({getOneValue, Name, Date, Type}, _, Monitor) ->
  Result = pollution:getOneValue(Name, Date, Type, Monitor),
  handleCall(Result,
    {no_station, "Cannot get value. Cannot find station with that name.~n", []},
    {no_value, "Cannot get value. There is no such measurement.~n", []},
    {"The result of the measurement is ~w.~n", [Result]}, Monitor);

handle_call({getStationMean, Name, Type}, _, Monitor) ->
  Result = pollution:getStationMean(Name, Type, Monitor),
  handleCall(Result,
    {no_station, "Cannot remove value. Cannot find station with that name.~n", []}, {[], [], []},
    {"Station mean is equal to ~w.~n", [Result]}, Monitor);

handle_call({getDailyMean, Day, Type}, _, Monitor) ->
  Mean = pollution:getDailyMean(Day, Type, Monitor),
  handleCall(Mean, {[], [], []}, {[], [], []}, {"Daily mean is equal to ~w.~n", [Mean]}, Monitor);

handle_call({getStationWithTheMostMeasurements}, _, Monitor) ->
  {Result1, Result2} =  pollution:getStationWithTheMostMeasurements(Monitor),
  handleCall({Result1, Result2},
    {empty_monitor,"Cannot get the station. Empty monitor.~n", []}, {[], [], []},
    {"The most measurements has the station (~w,~w).~n", [Result1, Result2]}, Monitor);

handle_call({getNearestStation, Coordinates}, _, Monitor) ->
  {Result1, Result2} = pollution:getNearestStation(Coordinates, Monitor),
  handleCall({Result1, Result2},
    {no_station, "There is no station with such coordinates.~n", []},
    {just_one_station, "Cannot find the closest station. There is just one in the monitor.~n", []},
    {"The closest station is (~w,~w).~n", [Result1, Result2]}, Monitor).


terminate(Reason, Monitor) ->
  io:format("~nServer: exit with state of monitor: ~p~n", [Monitor]),
  io:format("Server: exit with reason: ~p~n~n", [Reason]),
  Reason.