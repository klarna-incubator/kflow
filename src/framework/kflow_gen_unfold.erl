%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This behavior can be seen as the opposite of {@link
%%% kflow_gen_aggregate}; it implements a stream processing node that
%%% applies a pure function to each incoming message. This function
%%% produces a list of messages that should be sent downstream.
%%%
%%% This behavior can be used in two modes: full and simplified. In
%%% simplified mode stream processing node is defined like following:
%%%
%%% ```{unfold, fun(Offset, Message) -> [Message, Message] end}'''
%%%
%%% In full mode one has to create a callback module with
%%% `kflow_gen_unfold' behavior.
%%%
%%% `unfold' callback takes 3 arguments: first is offset of a message,
%%% second is the message itself and the third one is state of the
%%% callback module. This state is created in `init' callback and
%%% remains the same through the lifetime of the pipe. Return value of
%%% `unfold' callback is a list of messages, each one is passed
%%% downstream.
%%%
%%% `init' and `terminate' callbacks can be used e.g. when some
%%%  resource should be obtained to process messages. Both callbacks
%%%  are optional; configuration will be passed as is to
%%%  `map' callback when `init' is omitted.
%%%
%%% == Example ==
%%% ```
%%% -module(extract_writes).
%%%
%%% -behavior(kflow_gen_unfold).
%%%
%%% -export([init/1, unfold/3, terminate/1]).
%%%
%%% init(Config) ->
%%%   State = do_init(Config),
%%%   State.
%%%
%%% unfold(Offset, #tx{writes = Writes}, State) ->
%%%   Writes.
%%%
%%% terminate(State) ->
%%%   do_cleanup(State).
%%%
%%% '''
%%%
%%% NOTE: Since state is immutable, it's actually shared between the
%%% routes.
%%%
%%% @end
-module(kflow_gen_unfold).

-behavior(kflow_gen).

-include("kflow.hrl").
-include_lib("hut/include/hut.hrl").

-export([init/2, handle_message/3, handle_flush/2, terminate/2]).

-export_type([callback_fun/0]).

-callback init(_Config) -> _State.

-callback unfold(kflow:offset(), _DataIn, _State) -> [_DataOut].

-callback terminate(_State) -> _.

-optional_callbacks([init/1, terminate/1]).

-type callback_fun() :: fun((kflow:offset(), _InputMessage) -> [_OutputMessage]).

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
  #kflow_msg{ payload                = Payload0
            , offset                 = Offset
            , fully_processed_offset = FPO
            , route                  = Route
            } = Msg0,
  Payloads = case State of
               #s1{cb_module = CbModule, cb_state = CbState} ->
                 CbModule:unfold(Offset, Payload0, CbState);
               #s2{function = Fun} ->
                 Fun(Offset, Payload0)
             end,
  Msgs = create_downstream_messages(Offset, FPO, Route, Payloads),
  {ok, Msgs, State}.

%% @private
handle_flush(State, _) ->
  {ok, [], State}.

%% @private
terminate(#s1{cb_state = CbState, cb_module = CbModule}, _) ->
  kflow_lib:optional_callback(CbModule, terminate, [CbState]);
terminate(#s2{}, _) ->
  ok.

%% @private
-spec create_downstream_messages( kflow:offset()
                                , kflow:offset() | undefined
                                , kflow:route()
                                , term()
                                ) -> [#kflow_msg{}].
create_downstream_messages(Offset, FPO, Route, []) ->
  %% Callback didn't produce anything; create a hidden message to
  %% advance offset:
  [#kflow_msg{ offset                 = Offset
             , fully_processed_offset = FPO
             , hidden                 = true
             , route                  = Route
             }];
create_downstream_messages(Offset, FPO, Route, [Payload]) ->
  %% This is the last message; it can advance the offset as usual:
  [#kflow_msg{ offset                 = Offset
             , fully_processed_offset = FPO
             , hidden                 = false
             , route                  = Route
             , payload                = Payload
             }];
create_downstream_messages(Offset, FPO, Route, [Payload|Rest]) ->
  %% We cannot advance fully processed offset until the last message
  %% is produced; otherwise replays will lose data!
  [#kflow_msg{ offset                 = Offset
             , fully_processed_offset = min(FPO, Offset - 1) % works for `FPO=undefined'
             , hidden                 = false
             , route                  = Route
             , payload                = Payload
             } | create_downstream_messages(Offset, FPO, Route, Rest)].
