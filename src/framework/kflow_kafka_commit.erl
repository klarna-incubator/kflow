%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This stream processing node commits fully processed offsets
%%% to Kafka
%%% @end
%%%===================================================================
-module(kflow_kafka_commit).

-behavior(kflow_gen).

-include("kflow_int.hrl").

%% kflow_gen callbacks:
-export([ init/2
        , handle_message/3
        , handle_flush/2
        , terminate/2
        ]).

%%%===================================================================
%%% Types
%%%===================================================================

-type commit_fun() :: fun((brod:offset()) -> _).

-type state() ::
        #{ commit_fun            := commit_fun()
         , topic                 := brod:topic()
         , partition             := brod:partition()
         , group_id              := brod:group_id()
         , last_committed_offset := brod:offset()
         }.

%%%===================================================================
%%% `kflow_gen' callbacks
%%%===================================================================

%% @private
init(_NodeId, {_CbModule, InitInfo}) ->
  prometheus_gauge:declare([ {name, <<"kflow_kafka_offset_committed">>}
                           , {help, "Group subscriber offset that has been committed to Kafka"}
                           , {labels, [group_id, topic, partition]}
                           ]),
  prometheus_gauge:declare([ {name, <<"kflow_kafka_offset_commit_time">>}
                           , {help, "POSIX time when the offset was committed to Kafka"}
                           , {labels, [group_id, topic, partition]}
                           ]),
  {ok, InitInfo#{last_committed_offset => -1}}.

%% @private
handle_message( Msg = #kflow_msg{ offset                 = Offset
                                , fully_processed_offset = FPO
                                , route                  = Route
                                }
              , State
              , _
              ) ->
  #{ commit_fun            := CommitFun
   , topic                 := Topic
   , partition             := Partition
   , group_id              := GroupId
   , last_committed_offset := LastCommittedOffset
   } = State,
  SafeOffset = case FPO of
                 undefined -> Offset;
                 _         -> FPO
               end,
  %% Assert:
  SafeOffset =< Offset orelse
    ?slog(alert, #{ what           => "Offset tracking bug!"
                  , safe_offset    => SafeOffset
                  , message_offset => Offset
                  }),
  if SafeOffset >= LastCommittedOffset ->
      ok = CommitFun(SafeOffset),
      prometheus_gauge:set( <<"kflow_kafka_offset_committed">>
                          , [GroupId, Topic, Partition]
                          , SafeOffset
                          ),
      prometheus_gauge:set( <<"kflow_kafka_offset_commit_time">>
                          , [GroupId, Topic, Partition]
                          , erlang:system_time(second)
                          ),
      ?tp(info, kflow_kafka_commit_offset, #{ group_id  => GroupId
                                            , topic     => Topic
                                            , partition => Partition
                                            , offset    => SafeOffset
                                            });
     SafeOffset =:= LastCommittedOffset ->
      ok;
     %% Bug:
     true ->
      ?slog(alert, #{ what                   => "Offset tracking bug!"
                    , message_offset         => Offset
                    , fully_processed_offset => FPO
                    , last_committed_offset  => LastCommittedOffset
                    , group_id               => GroupId
                    , route                  => Route
                    })
  end,
  {ok, [Msg], State#{last_committed_offset => SafeOffset}}.

%% @private
handle_flush(State, _) ->
  {ok, [], State}.

%% @private
terminate(_, _) ->
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
