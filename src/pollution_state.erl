%%%-------------------------------------------------------------------
%% @doc separate process only for keeping state
%% @end
%%%-------------------------------------------------------------------
-module(pollution_state).
-author("Agnieszka Dutka").
-behaviour(gen_server).

%% API
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).
-export([start/0, stop/0]).

start()   ->
  gen_server:start_link({local,?MODULE},?MODULE, pollution:createMonitor(),[]).

stop()     -> gen_server:call(?MODULE, terminate).

init(StartMonitor) ->   {ok, StartMonitor}.

handle_call(get, _From, State) -> {reply, State, State};
handle_call(terminate,_From, State) -> {stop, normal, ok, State}.

handle_cast({update, NewState}, _State) ->
  {noreply, NewState};

handle_cast({wipe}, _State) ->
  {noreply, pollution:createMonitor()}.

terminate(normal, _) -> io:format("---properly terminating state----~n",[]), ok;
terminate(_, _) -> io:format("Hm, state is terminating. Not good ~n",[]), ok.
