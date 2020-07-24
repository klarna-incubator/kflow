-module(kflow_trace_specs).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([ message_conservation/3
        , check_aggregation/2
        , state_continuity/2
        , message_order/2
        , init_terminate/1
        , replicate/2
        ]).

%%====================================================================
%% Macros
%%====================================================================

%% Given an upstream event `UPSTREAM', find a complementary event that
%% is emitted at my layer, matching `DOWNSTREAM' pattern:
-define(find_downstream(UPSTREAM, DOWNSTREAM, GUARD, TRACE),
        snabbkaffe:find_pairs( _Strict = true
                               %% Match upstream event:
                             , fun(__A) -> __A =:= (UPSTREAM) end
                               %% Match my event:
                             , fun(__A) ->
                                   case __A of
                                     DOWNSTREAM ->
                                       true;
                                     _ ->
                                       false
                                   end
                               end
                               %% Guard:
                               , (GUARD)
                             , (TRACE)
                             )).

-define(find_downstream(UPSTREAM, DOWNSTREAM, TRACE),
        ?find_downstream(UPSTREAM, DOWNSTREAM, fun(_, _) -> true end, TRACE)).

%%====================================================================
%% Trace validation functions
%%====================================================================

%% Check that in the absense of crashes all messages get relayed from
%% one layer to another without loss:
message_conservation(PipeDefn, Messages, Trace) ->
  InputMessages = ?of_kind(test_feed_message, Trace),
  lists:foreach( fun(Msg) ->
                     %% Check that for each message that we feed into
                     %% the pipe there is an effect on each layer of
                     %% the pipe:
                     do_check_message_conservation(PipeDefn, Msg, Trace)
                 end
               , InputMessages),
  true.

%% Check that aggregate node emits one `out' trace per `Size' `in'
%% messages:
check_aggregation(#{id := Id, size := Size0}, Trace) ->
  %% Edge case: since aggregator has to flush _something_ `Size == 0'
  %% is equivalent to `Size == 1':
  Size = max(1, Size0),
  %% Find relevant messages emmitted by this aggregator:
  Msgs = ?projection(?snk_kind, [I || I = #{?snk_kind := Kind, id := _Id} <- Trace,
                                      _Id =:= Id,
                                      Kind =:= kflow_test_aggregate_out orelse
                                      Kind =:= kflow_test_aggregate_in]),
  NumMsgs = length(Msgs),
  if NumMsgs > 0 ->
      %% Create a reference term to compare trace with. E.g. for
      %% aggregate with Size=2 it should look like:
      %% ```[in, in, out, in, in, out, in ...]'''
      NumChunks = NumMsgs div (Size + 1),
      %% 1. Create an `[in, in, out]' term:
      Expected1 = replicate(Size, kflow_test_aggregate_in) ++
                    [kflow_test_aggregate_out],
      %% 2. Replicate it as many times as needed to match the input:
      Expected2 = lists:append(replicate(NumChunks + 1, Expected1)),
      %% 3. Cut it up to size.
      %% Note: the last message in the trace should always be an
      %% `out', since `gen_aggregate' flushes on stopping. Let's
      %% forget about it for now, hence `NumMsgs - 1'...
      {Expected3, _} = lists:split(NumMsgs - 1, Expected2),
      %% But we add it to the reference term here:
      Expected = Expected3 ++ [kflow_test_aggregate_out],
      %% Compare trace with the reference
      ?assertEqual(Expected, Msgs, #{id => Id, size => Size});
     true ->
      %% All messages were filtered by the upstream, nothing to see
      %% here
      ok
  end.

%% Check that state of a callback module is handled in a consistent
%% manner, never lost or discarded. This is done by checking that
%% `counter' field increases monotonically after every event (the
%% callback module should advance `counter' in its state on each
%% callback invocation to make this work)
state_continuity(#{id := Id}, Trace) ->
  FilteredTrace = [X || X = #{id := Id_} <- Trace, Id_ =:= Id],
  Chunks = ?splitl_trace(#{?snk_kind := kflow_test_init}, FilteredTrace),
  lists:foreach( fun(Chunk) ->
                     state_continuity1(Id, Chunk)
                 end
               , Chunks
               ).

state_continuity1(Id, Trace) ->
  States = [C || #{counter := C} <- Trace],
  Expected = lists:seq(0, length(States) - 1),
  ?assertEqual(Expected, States, #{id => Id}).

%% Check that offset of input messages is stricly increasing on each
%% layer of pipe:
message_order(PipeDefn, Trace) ->
  Fun = fun({Meta, CB, #{id := Id}}) ->
            Kind = case {Meta, CB} of
                     {map, kflow_test_map} ->
                       kflow_test_map_seen_message;
                     {filter, kflow_test_filter} ->
                       kflow_test_filter_message;
                     {mfd, kflow_test_filter} ->
                       kflow_test_filter_message;
                     {aggregate, kflow_test_aggregate} ->
                       kflow_test_aggregate_in;
                     {unfold, kflow_test_unfold} ->
                       kflow_test_unfold_seen_message
                   end,
            do_check_message_order(Kind, Id, Trace)
        end,
  lists:foreach(Fun, PipeDefn),
  true.

init_terminate(Trace) ->
  ?strict_causality( #{?snk_kind := kflow_test_init, id := Id}
                   , #{?snk_kind := kflow_test_terminate, id := Id}
                   , Trace
                   ).

%%====================================================================
%% Internal functions
%%====================================================================

%% Check what happens to the message on each layer:
do_check_message_conservation([], _, _) ->
  true;
do_check_message_conservation( [{map, kflow_test_map, Config}|Rest]
                             , UpstreamMsg
                             , Trace
                             ) ->
  #{id := MyId} = Config,
  #{offset := Offset, payload := Payload} = UpstreamMsg,
  %% Given an upstream event `Msg', find a corresponding event `Msg2'
  %% that is emitted at my layer:
  Pairs = ?find_downstream( UpstreamMsg
                          , #{ ?snk_kind := kflow_test_map_seen_message
                             , id        := MyId
                             , offset    := Offset
                             }
                          , Trace
                          ),
  %% One pair of events should be present:
  [{pair, UpstreamMsg, MyMsg}] = Pairs,
  %% Payload should be passed down unchanged
  ?assertEqual(Payload, maps:get(payload, MyMsg)),
  %% Check the downstream:
  do_check_message_conservation(Rest, MyMsg, Trace);
do_check_message_conservation( [{FilterBehavior, kflow_test_filter, Config}|Rest]
                             , UpstreamMsg
                             , Trace
                             ) when FilterBehavior =:= filter;
                                    FilterBehavior =:= mfd ->
  #{id := MyId} = Config,
  #{offset := Offset} = UpstreamMsg,
  Pairs = ?find_downstream( UpstreamMsg
                          , #{ ?snk_kind := kflow_test_filter_message
                             , offset    := Offset
                             , id        := MyId
                             }
                          , Trace
                          ),
  %% One pair of events should be present:
  [{pair, UpstreamMsg, MyMsg}] = Pairs,
  case MyMsg of
    #{alive := true} ->
      %% The message survived, check if downstream got it:
      do_check_message_conservation(Rest, MyMsg, Trace);
    #{alive := false} ->
      %% The message was eliminated, check that the downstream nodes
      %% didn't get it. Check that only the current layer's message
      %% got matched:
      ?assertMatch( [{singleton, _}]
                  , ?find_downstream( MyMsg
                                    , #{ offset := Offset
                                       , id     := Id1
                                       } when Id1 > MyId
                                    , Trace
                                    ))
  end;
do_check_message_conservation( [{aggregate, kflow_test_aggregate, Config}|Rest]
                             , UpstreamMsg
                             , Trace
                             ) ->
  #{id := MyId} = Config,
  #{offset := Offset, payload := ExpectedPayload} = UpstreamMsg,
  %% Given an upstream message find an `in' event at my level:
  Pairs = ?find_downstream( UpstreamMsg
                          , #{ ?snk_kind := kflow_test_aggregate_in
                             , id        := MyId
                             , offset    := Offset
                             }
                          , Trace
                          ),
  [{pair, UpstreamMsg, MyMsg}] = Pairs,
  #{flush := Flush, payload := Payload} = MyMsg,
  ?assertEqual(ExpectedPayload, Payload),
  case Flush of
    flush ->
      %% This message was the last buffered message before the
      %% flush. It means there should be an `out' message with the
      %% same offset:
      Out = ?find_downstream( MyMsg
                            , #{ ?snk_kind := kflow_test_aggregate_out
                               , id        := MyId
                               , offset    := Offset
                               }
                            , Trace
                            ),
      [{pair, MyMsg, OutMsg}] = Out,
      %% TODO Replace with proper:aggregate?
      snabbkaffe:push_stat(kflow_test_aggregate_flush, 1),
      %% Check that `out' message was propagated to the lower levels:
      do_check_message_conservation(Rest, OutMsg, Trace);
    keep ->
      %% TODO Replace with proper:aggregate?
      snabbkaffe:push_stat(kflow_test_aggregate_flush, 0),
      true
  end;
do_check_message_conservation( [{unfold, kflow_test_unfold, Config}|Rest]
                             , UpstreamMsg
                             , Trace
                             ) ->
  #{id := MyId} = Config,
  #{offset := Offset, payload := Payload} = UpstreamMsg,
  %% Given an upstream event `Msg', find a corresponding event `Msg2'
  %% that is emitted at my layer:
  Pairs = ?find_downstream( UpstreamMsg
                          , #{ ?snk_kind := kflow_test_unfold_seen_message
                             , id        := MyId
                             , offset    := Offset
                             }
                          , Trace
                          ),
  %% One pair of events should be present:
  [{pair, UpstreamMsg, MyMsg}] = Pairs,
  %% Payload should be passed down unchanged
  ?assertEqual(Payload, maps:get(payload, MyMsg)),
  %% Check the downstream:
  do_check_message_conservation(Rest, MyMsg, Trace).

-spec replicate(non_neg_integer(), A) -> [A].
replicate(0, _) ->
  [];
replicate(N, A) when N > 0 ->
  [A | replicate(N - 1, A)].

%% Check that offset is strictly increasing:
do_check_message_order(Kind, Id, Trace) ->
  Offsets = [Offset || #{id := Id_, ?snk_kind := Kind_, offset := Offset} <- Trace
                     , Kind_ =:= Kind, Id_ =:= Id],
  snabbkaffe:strictly_increasing(Offsets).
