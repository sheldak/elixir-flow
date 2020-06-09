%%%-------------------------------------------------------------------
%% @doc pollution top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pollution_sup).
-author("Samuel Heldak").

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  Monitor = pollution:createMonitor(),
  supervisor:start_link({local, pollutionSupervisor}, ?MODULE, Monitor).

init(InitialMonitor) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 3,
                 period => 3},

    ChildSpecs = [#{id => 'pollution',
                    start => {pollution_gen_server, start, [InitialMonitor]},
                    restart => permanent,
                    shutdown => 2000,
                    type => worker,
                    modules => [pollution_gen_server]}],

    {ok, {SupFlags, ChildSpecs}}.
