%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This stateless stream processing node splits stream of
%%% messages into multiple sub-streams according to an output value of
%%% a callback.
%%%
%%% == Example ==
%%% ```
%%% -module(my_demux).
%%%
%%% -behavior(kflow_gen_demux).
%%%
%%% -export([init/1, demux/3, terminate/1]).
%%%
%%% init(Config) ->
%%%   State = do_init(Config),
%%%   State.
%%%
%%% demux(Offset, Message, State) ->
%%%   maps:get(key, Message, default).
%%%
%%% terminate(State) ->
%%%   do_cleanup(State).
%%%
%%% '''
%%%
%%% @end
%%%===================================================================
-module(kflow_gen_demux).

-behavior(kflow_gen).

-include("kflow_int.hrl").

%% kflow_gen callbacks:
-export([ init/2
        , handle_message/3
        , handle_flush/2
        , terminate/2
        ]).

-export_type([callback_fun/0]).

-callback init(_Config) -> _State.

-callback demux(kflow:offset(), _DataIn, _State) -> _Route.

-callback terminate(_State) -> _.

-optional_callbacks([init/1, terminate/1]).

-type callback_fun() :: fun((kflow:offset(), _Message) -> _Route).

-record(s1,
        { cb_module :: module()
        , cb_state  :: term()
        }).

-record(s2,
        { function :: callback_fun()
        }).

%%%===================================================================
%%% `kflow_gen' callbacks
%%%===================================================================

%% @private
init(_NodeId, {?MODULE, Fun}) when is_function(Fun) ->
  is_function(Fun, 2) orelse error({badarity, Fun}),
  {ok, #s2{ function = Fun
          }};
init(_NodeId, {CbModule, CbConfig}) ->
  CbState = kflow_lib:optional_callback(CbModule, init, [CbConfig], CbConfig),
  {ok, #s1{ cb_module = CbModule
          , cb_state  = CbState
          }}.

%% @private
handle_message(Msg = #kflow_msg{hidden = true}, State, _) ->
  %% Don't execute callback for a hidden message, simply pass it
  %% downstream:
  {ok, [Msg], State};
handle_message(Msg0, State, _) ->
  #kflow_msg{ payload = Payload
            , offset  = Offset
            , route   = Route0
            } = Msg0,
  NewRoute = case State of
               #s1{cb_module = CbModule, cb_state = CbState} ->
                 CbModule:demux(Offset, Payload, CbState);
               #s2{function = Fun} ->
                 Fun(Offset, Payload)
             end,
  Msg = Msg0#kflow_msg{route = [NewRoute|Route0]},
  {ok, [Msg], State}.

%% @private
handle_flush(State, _) ->
  {ok, [], State}.

%% @private
terminate(#s1{cb_state = CbState, cb_module = CbModule}, _) ->
  kflow_lib:optional_callback(CbModule, terminate, [CbState]);
terminate(#s2{}, _) ->
  ok.
