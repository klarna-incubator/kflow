%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @private
%%%===================================================================
-module(kflow_gen_aggregate_impl).

-behavior(kflow_multistate).

-include("kflow_int.hrl").

%% kflow_multistate callbacks:
-export([ init/3
        , handle_message/3
        , handle_flush/2
        , terminate/2
        , fully_processed_offset/1
        ]).

-record(s,
        { cb_module   :: module()
        , cb_state    :: term()
        , msgs_in = 0 :: integer()
          %% Offset that is safe to commit:
        , safe_offset :: kflow:offset() | undefined
          %% Offset of the last seen message:
        , offset = 0  :: kflow:offset()
        , route       :: kflow:route()
        }).

-type state() :: #s{}.

%% @private
init(_NodeId, _Route, {CbModule, CbConfig}) ->
  CbState = kflow_lib:optional_callback( CbModule
                                       , init
                                       , [CbConfig]
                                       , undefined
                                       ),
  {ok, #s{ cb_module = CbModule
         , cb_state  = CbState
         , msgs_in   = 0
         }}.

%% @private
handle_message(Msg, State0, Config = {_, CbConfig}) ->
  #kflow_msg{ payload = Payload
            , offset  = Offset
            , route   = Route
            , hidden  = false % Assert
            } = Msg,
  #s{ cb_module   = CbModule
    , cb_state    = CbState0
    , msgs_in     = N
    , offset      = OldOffset
    , safe_offset = OldSafeOffset
    } = State0,
  %% Check if it's the first buffered message and remember its offset,
  %% or leave fully processed offset alone:
  SafeOffset = case N of
                 0 -> Offset - 1;
                 _ -> OldSafeOffset
               end,
  {Flush, CbState1} = CbModule:in(Offset, Payload, CbState0, CbConfig),
  case Flush of
    keep ->
      State = State0#s{ cb_state    = CbState1
                      , msgs_in     = N + 1
                      , offset      = Offset
                      , safe_offset = SafeOffset
                      , route       = Route
                      },
      {ok, [], State};
    reject when N > 0 ->
      %% The new message is incompatible with what we've buffered so
      %% far. Flush the existing state (`CbState0') and replay the
      %% last message from a clean state.
      %% TODO: Here we ignore request to terminate callback
      %% module. It's wrong, but hopefully no one does this weird thing
      {_, PayloadDownstream1, CbState2} = CbModule:out(CbState0, CbConfig),
      MsgDownstream1 = #kflow_msg{ payload = PayloadDownstream1
                                 , offset  = OldOffset
                                 , route   = Route
                                 },
      State1 = State0#s{ cb_state = CbState2
                       , msgs_in  = 0
                       , route    = Route
                       },
      %% Now repeat the message:
      {RetType, MsgsDownstream, State} = handle_message(Msg, State1, Config),
      {RetType, [MsgDownstream1|MsgsDownstream], State};
    _ when Flush =:= flush; Flush =:= reject ->
      {RetType, PayloadDownstream, CbState2} = CbModule:out(CbState1, CbConfig),
      State = State0#s{ cb_state    = CbState2
                      , msgs_in     = 0
                      , offset      = Offset
                      , route       = Route
                      , safe_offset = undefined
                      },
      MsgDownstream = #kflow_msg{ payload = PayloadDownstream
                                , offset  = Offset
                                , route   = Route
                                },
      {RetType, [MsgDownstream], State}
  end.

%% @private
handle_flush(State = #s{msgs_in = 0}, _) ->
  %% Nothing to flush:
  {ok, [], State};
handle_flush(State0, {_, CbConfig}) ->
  #s{ cb_module = CbModule
    , cb_state  = CbState0
    , offset    = Offset
    , route     = Route
    } = State0,
  case CbModule:out(CbState0, CbConfig) of
    {RetType, PayloadDownstream, CbState1} ->
      MsgDownstream = #kflow_msg{ payload = PayloadDownstream
                                , offset  = Offset
                                , route   = Route
                                },
      State = State0#s{ cb_state    = CbState1
                      , msgs_in     = 0
                      , safe_offset = undefined
                      },
      {RetType, [MsgDownstream], State};
    keep ->
      %% User callback module refuses to flush; in this case we don't
      %% do anything:
      {ok, [], State0}
  end.

%% @private
terminate(#s{cb_state = CbState, cb_module = CbModule}, {_, CbConfig}) ->
  kflow_lib:optional_callback(CbModule, terminate, [CbState, CbConfig]).

%% @private
fully_processed_offset(#s{safe_offset = Offset}) ->
  Offset.
