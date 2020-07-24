%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This stateless stream processing node joins sub-streams. It
%%% doesn't have any parameters.
%%%
%%% @end
%%%===================================================================
-module(kflow_join).

-behavior(kflow_gen).

-include("kflow_int.hrl").

%% kflow_gen callbacks:
-export([ init/2
        , handle_message/3
        , handle_flush/2
        , terminate/2
        ]).

%%%===================================================================
%%% `kflow_gen' callbacks
%%%===================================================================

%% @private
init(_NodeId, _) ->
  {ok, void}.

%% @private
handle_message(Msg0, State, _) ->
  #kflow_msg{route = Route} = Msg0,
  Msg = Msg0#kflow_msg{route = tl(Route)},
  {ok, [Msg], State}.

%% @private
handle_flush(State, _) ->
  {ok, [], State}.

%% @private
terminate(_, _) ->
  ok.
