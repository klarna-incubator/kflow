%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc `kflow_multistate' is a module that helps keeping track of
%%% multiple states of an intermediate callback module. It wraps
%%% around a stateful intermediate CBM and multiplexes its state
%%% according to the value of `route' field of upstream messages. It
%%% also rewrites `fully_processed_offset' field of the downstream
%%% messages to make sure all unacked messages of all routes get
%%% replayed on pipe restart.
%%%
%%% Stateful intermediate CBMs using must implement `kflow_multistate'
%%% behavior.
%%%
%%% NOTE: `fully_processed_offset/1' callback must be a pure function.
%%%
%%% NOTE: It doesn't make much sense to use this wrapper for stateless
%%% modules.
%%%
%%% NOTE: Callback module that wraps around a callback module may
%%% sound like a terrible idea. But in reality it's the only sane way
%%% to keep complexity of already difficult-to-understand modules such
%%% as `kflow_gen' or `kflow_gen_aggregate' under control. This way
%%% different functionality is contained in separate places.
%%%
%%% @end
%%%===================================================================
-module(kflow_multistate).

-include("kflow_int.hrl").

%% API;
-export([wrap/3, handle_message/3, handle_flush/2, terminate/2]).

-export_type([wrapped/0]).

%%%===================================================================
%%% Types and macros:
%%%===================================================================

-type route_states() :: #{kflow:route() => term()}.

-record(s,
        { module       :: module()
        , node_id      :: kflow:node_id()
        , states = #{} :: route_states()
        , config       :: term()
        }).

-opaque wrapped() :: #s{}.

%%%===================================================================
%%% Callback definitions:
%%%===================================================================

%% Initialize the callback module:
-callback init(kflow:node_id(), kflow:route(), _Config) ->
  {ok, _State}.

%% Handle message from the upstream:
-callback handle_message(kflow:message(), State, _Config) ->
  kflow_gen:callback_return(State).

%% Handle flush:
-callback handle_flush(State, _Config) ->
  kflow_gen:callback_return(State).

%% Handle graceful shutdown:
-callback terminate(_State, _Config) -> _.

-callback fully_processed_offset(_State) -> kflow:offset()
                                          | undefined.

%%%===================================================================
%%% API functions:
%%%===================================================================

%% @doc Wrap intermediate callback module
-spec wrap(kflow:node_id(), module(), term()) -> {ok, wrapped()}.
wrap(NodeId, IntermediateCbModule, Config) ->
  {ok, #s{ module  = IntermediateCbModule
         , node_id = NodeId
         , config  = Config
         }}.

%% @doc Wrap `handle_message' callback
-spec handle_message(kflow:message(), wrapped(), _Config) ->
                        kflow_gen:callback_return(wrapped()).
handle_message(Msg = #kflow_msg{hidden = true}, State, _) ->
  %% This module takes care of hidden messages, this makes state
  %% tracking less of a pain.
  #s{ module = CbModule
    , states = RouteStates
    } = State,
  case fully_processed_offset(undefined, CbModule, RouteStates) of
    undefined ->
      %% There's no buffered data, hidden message can advance safe offset:
      {ok, [Msg], State};
    _ ->
      %% There's buffered data, don't advance offset:
      {ok, [], State}
  end;
handle_message(Msg, State0, _Config) ->
  #kflow_msg{ route = Route
            , offset = MsgOffset
            } = Msg,
  #s{ module = CbModule
    , states = RouteStates0
    , config = Config
    } = State0,
  State1 = maybe_init_cb_state(Route, State0, Config),
  CbState0 = maps:get(Route, State1#s.states),
  try CbModule:handle_message(Msg, CbState0, Config) of
    {RetType, Messages, CbState} when RetType =:= ok;
                                      RetType =:= exit ->
      RouteStates = maybe_terminate_cb_state( Route, CbState, RetType, RouteStates0
                                            , CbModule, Config),
      SafeOffset = fully_processed_offset(MsgOffset, CbModule, RouteStates),
      State = State1#s{states = RouteStates},
      {ok, rewrite_offsets(SafeOffset, Messages), State};
    WrongResult ->
      ?slog(critical, #{ what       => "Kflow node invalid callback return"
                       , route      => Route
                       , retur      => WrongResult
                       }),
      do_terminate(CbModule, CbState0, Config, Route),
      error(handler_error)
  catch EC:Err:Stack ->
      ?slog(critical, #{ what       => "Kflow node callback crash"
                       , route      => Route
                       , error      => {EC, Err}
                       , stacktrace => Stack
                       }),
      do_terminate(CbModule, CbState0, Config, Route),
      error(handler_error)
  end.

%% @doc Wrap `handle_flush' callback
-spec handle_flush(wrapped(), _Config) ->
                      kflow_gen:callback_return(wrapped()).
handle_flush(State0, _) ->
  #s{ module = CbModule
    , states = RouteStates0
    , config = Config
    } = State0,
  DoFlush = fun(Route, CbState0, {MsgsAcc, StatesAcc0}) ->
                {RetType, Msgs, CbState} = CbModule:handle_flush(CbState0, Config),
                StatesAcc = maybe_terminate_cb_state( Route
                                                    , CbState
                                                    , RetType
                                                    , StatesAcc0
                                                    , CbModule
                                                    , Config
                                                    ),
                {Msgs ++ MsgsAcc, StatesAcc}
            end,
  {Messages0, RouteStates} = maps:fold(DoFlush, {[], #{}}, RouteStates0),
  Messages = lists:keysort(#kflow_msg.offset, Messages0),
  Offset = fully_processed_offset(undefined, CbModule, RouteStates),
  State = State0#s{states = RouteStates},
  {ok, rewrite_offsets(Offset, Messages), State}.

%% @doc Wrap `terminate' callback
-spec terminate(wrapped(), _Config) -> _.
terminate(#s{module = Module, states = RouteStates, config = Config}, _) ->
  maps:map( fun(Route, State) ->
                do_terminate(Module, State, Config, Route)
            end
          , RouteStates
          ),
  void.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private Set fully processed offset of the outgoing messages to the
%% highest safe-to-commit value
-spec rewrite_offsets(kflow:offset() | undefined, [kflow:message()]) ->
                         [kflow:message()].
rewrite_offsets(Offset, Messages) ->
  %% The below code implicitly works for `FPO =:= undefined' via type
  %% coerction: atoms are "more" than numbers
  [Msg#kflow_msg{fully_processed_offset = min(MsgOffset, min(FPO, Offset))}
   || Msg = #kflow_msg{ fully_processed_offset = FPO
                      , offset                 = MsgOffset
                      } <- Messages].

%% @private Calculate offset that is safe to commit
-spec fully_processed_offset(kflow:offset() | undefined, module(), route_states()) ->
                                kflow:offset() | undefined.
fully_processed_offset(MsgOffset, Module, RouteStates) ->
  Fun = fun(_Route, State, Acc) ->
            case Module:fully_processed_offset(State) of
              undefined ->
                Acc;
              Offset when is_integer(Offset) ->
                %% The below line works with `undefined' via type
                %% coercion magic:
                min(Acc, Offset)
            end
        end,
  maps:fold(Fun, MsgOffset, RouteStates).

%% @private Init state for a route if not present
-spec maybe_init_cb_state(kflow:route(), #s{}, {module(), term()}) -> #s{}.
maybe_init_cb_state(Route, State, Config) ->
  #s{ node_id = NodeId
    , module  = CbModule
    , states  = RouteStates
    } = State,
  case RouteStates of
    #{Route := _} ->
      State;
    _ ->
      {ok, CbState} = CbModule:init(NodeId, Route, Config),
      State#s{states = RouteStates #{Route => CbState}}
  end.

%% @private Update state of the route, or terminate it if the callback module asks so
-spec maybe_terminate_cb_state( kflow:route()
                              , term()
                              , kflow_gen:ret_type()
                              , route_states()
                              , module()
                              , term()
                              ) -> route_states().
maybe_terminate_cb_state(Route, CbState, ok, RouteStates, _CbModule, _CbConfig) ->
  RouteStates #{Route => CbState};
maybe_terminate_cb_state(Route, CbState, exit, RouteStates, CbModule, CbConfig) ->
  do_terminate(CbModule, CbState, CbConfig, Route),
  maps:without([Route], RouteStates).

%% @private Call terminate callback and ignore any error.
-spec do_terminate(module(), term(), term(), kflow:route()) -> _.
do_terminate(Module, State, Config, Route) ->
  ?slog(debug, #{ what   => "Terminating a route"
                , module => Module
                , state  => State
                , route  => Route
                }),
  try Module:terminate(State, Config)
  catch EC:Error:Stack ->
      ?slog(error, #{ what       => "Crash in terminate callback"
                    , module     => Module
                    , error      => {EC, Error}
                    , stacktrace => Stack
                    , route      => Route
                    , state      => State
                    })
  end.
