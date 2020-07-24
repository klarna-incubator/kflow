%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module defines a stateless stream processing node that
%%% combines {@link kflow_gen_map}, {@link kflow_gen_filter} and
%%% {@link kflow_gen_demux}.
%%%
%%% This behavior can be used in two modes: full and simplified. In
%%% simplified mode stream processing node is defined like following:
%%%
%%% ```{mfd, fun(Offset, Message) -> {true, Message} | {true, Route, Message} | false end}'''
%%%
%%% In full mode one has to create a callback module with
%%% `kflow_gen_mfd' behavior.
%%%
%%% `mfd' callback takes 3 arguments: first is offset of a message,
%%% second is the message itself and the third one is state of the
%%% callback module. This state is created in `init' callback and
%%% remains the same through the lifetime of the pipe. Return value of
%%% `mfd' callback should be of type {@link return_type/0}.
%%%
%%% `init' and `terminate' callbacks can be used e.g. when some
%%% resource should be obtained to process messages. Both callbacks
%%% are optional; configuration will be passed as is to `filter'
%%% callback when `init' is omitted.
%%%
%%% == Example ==
%%% ```
%%% -module(my_mfd).
%%%
%%% -behavior(kflow_gen_mfd).
%%%
%%% -export([init/1, filtermap/3, terminate/1]).
%%%
%%% init(Config) ->
%%%   State = do_init(Config),
%%%   State.
%%%
%%% mfd(Offset, Message, State) ->
%%%   %% Apply `transform' to the message and pass it downstream:
%%%   {true, transform(Message)};
%%% mfd(Offset, Message, State) ->
%%%   %% Apply `transform' to the message and pass it to a substream `Route':
%%%   {true, Route, transform(Message)};
%%% mfd(Offset, Message, State) ->
%%%   %% Drop the message:
%%%   false.
%%%
%%% terminate(State) ->
%%%   do_cleanup(State).
%%% '''
%%%
%%% NOTE: Since state is immutable, it's actually shared between the
%%% routes.
%%%
%%% @end
%%%===================================================================
-module(kflow_gen_mfd).

-behavior(kflow_gen).

-include("kflow.hrl").
-include_lib("hut/include/hut.hrl").

-export([init/2, handle_message/3, handle_flush/2, terminate/2]).

-export_type([callback_fun/0, return_type/0]).

-type return_type() :: {true, Ret :: term()}
                     | {true, Route :: term(), Ret :: term()}
                     | false.

-callback init(_Config) -> _State.

-callback mfd(kflow:offset(), _DataIn, _State) ->
  {true, _DataOut} | false.

-callback terminate(_State) -> _.

-optional_callbacks([init/1, terminate/1]).

-type callback_fun() :: fun((kflow:offset(), _Message) -> return_type()).

-record(s1,
        { cb_module :: module()
        , cb_state  :: term()
        }).

-record(s2,
        { function :: callback_fun()
        }).

-type state() :: #s1{} | #s2{}.

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
  %% Don't execute callback for a hidden message, simply pass it downstream:
  {ok, [Msg], State};
handle_message(Msg0, State, _) ->
  #kflow_msg{payload = Payload, offset = Offset, route = Route0} = Msg0,
  Ret = case State of
          #s1{cb_module = CbModule, cb_state = CbState} ->
            CbModule:mfd(Offset, Payload, CbState);
          #s2{function = Fun} ->
            Fun(Offset, Payload)
        end,
  Msg = case Ret of
          {true, NewPayload} ->
            Msg0#kflow_msg{payload = NewPayload};
          {true, Route, NewPayload} ->
            Msg0#kflow_msg{payload = NewPayload, route = [Route|Route0]};
          false ->
            Msg0#kflow_msg{hidden = true}
        end,
  {ok, [Msg], State}.

%% @private
handle_flush(State, _) ->
  {ok, [], State}.

%% @private
terminate(#s1{cb_state = CbState, cb_module = CbModule}, _) ->
  kflow_lib:optional_callback(CbModule, terminate, [CbState]);
terminate(#s2{}, _) ->
  ok.
