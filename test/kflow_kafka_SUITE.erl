-module(kflow_kafka_SUITE).

-compile(export_all).

-include_lib("kflow/src/testbed/kafka_ct_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_suite(Config) ->
  kflow_kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) ->
  ok.

suite() -> [{timetrap, {seconds, 300}}].

common_init_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config).

common_end_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_end_per_testcase(Case, Config).

%%====================================================================
%% Testcases
%%====================================================================

%% Check that kflow pipes process messages from all partitions in the
%% topic:
t_basic_consume({pipe_config, _}) -> [];
t_basic_consume(num_partitions) -> 5;
t_basic_consume(_Config) when is_list(_Config) ->
  NMessages  = 100,
  MaxOffset  = NMessages - 1,
  Partitions = lists:seq(0, 4),
  ?check_trace(
     begin
       %% Start consumer pipe:
       PipeSpec = [ {map, fun(_, Msg) ->
                              ?log(debug, "Debug: ~p", [Msg]),
                              ?log(notice, "Notice: ~p", [Msg]),
                              Msg
                          end}
                  , {map, kflow_test_map, #{id => '1'}}
                  ],
       Spec = #{ group_id  => ?group_id
               , topics    => [?topic]
               , id        => ?FUNCTION_NAME
               , pipe_spec => PipeSpec
               },
       [produce({?topic, P}, integer_to_binary(I))
        || P <- Partitions
         , I <- lists:seq(1, NMessages)
         ],
       %% Start consumer _after_ producing the messages to make sure
       %% it starts consuming from the earliest offset:
       {ok, Pid} = kflow_kafka_consumer_v2:start_link(Spec),
       [{ok, _} = ?block_until(#{ ?snk_kind := kflow_kafka_commit_offset
                                , partition := P
                                , offset    := MaxOffset
                                }, infinity, infinity)
        || P <- Partitions],
       kflow_kafka_consumer_v2:stop(Pid)
     end,
     fun(_, Trace) ->
         message_conservation(Trace, '1'),
         message_ordering(Trace, '1', Partitions),
         kflow_trace_specs:init_terminate(Trace),
         %% Check offsets:
         ExpectedOffsets = [{P, MaxOffset} || P <- Partitions],
         ?retry( 1000, 10
               , check_committed_offsets(?group_id(t_basic_consume), ExpectedOffsets)
               )
     end).

%% Verify that crashes in the callback module still produce complete
%% stream of events
t_pipe_crash({pipe_config, _}) ->
  PipeSpec = [ {map, fun(_, Msg) ->
                         ?maybe_crash(test_maybe_crash, #{}),
                         Msg
                     end}
             , {map, kflow_test_map, #{id => '1'}}
             ],
  Args = #{ group_id        => ?group_id
          , topics          => [?topic]
          , id              => ?FUNCTION_NAME
          , pipe_spec       => PipeSpec
          , consumer_config => consumer_config()
          },
  [#{ start => {kflow_kafka_consumer_v2, start_link}
    , args  => Args
    }];
t_pipe_crash(num_partitions) ->
  1;
t_pipe_crash(_Config) when is_list(_Config) ->
  NMessages  = 30,
  MaxOffset  = NMessages - 1,
  Partitions = [0],
  ?check_trace(
     #{timeout => 10000},
     %% Run stage:
     begin
       ?inject_crash( #{?snk_kind := test_maybe_crash}
                    , snabbkaffe_nemesis:periodic_crash(10, 0.9, 0)
                    ),
       [produce({?topic, 0}, integer_to_binary(I))
        || I <- lists:seq(1, NMessages)],
       {ok, _} = ?block_until(#{ ?snk_kind      := kflow_kafka_commit_offset
                               , offset    := MaxOffset
                               }, 30000, infinity),
       application:stop(kflow)
     end,
     %% Check stage:
     fun(_, Trace) ->
         %% Verify that all messages were received by the last node:
         Received = ?projection( offset
                               , ?of_kind(kflow_test_map_seen_message, Trace)
                               ),
         ?assertEqual( lists:seq(0, MaxOffset)
                     , lists:usort(Received)
                     ),
         NCrashes = length(?of_kind(snabbkaffe_crash, Trace)),
         %% Verify offsets committed to Kafka:
         ExpectedOffsets = [{P, MaxOffset} || P <- Partitions],
         ?retry( 1000, 10
               , check_committed_offsets(?group_id, ExpectedOffsets)
               ),
         ?log(notice, "Number of injected crashes: ~p", [NCrashes]),
         kflow_trace_specs:init_terminate(Trace)
     end).


%% Verify that adding consumer group members leads to calling
%% terminate callback for each worker process. TODO: add more
%% rebalance tests
t_rebalance({pipe_config, _}) -> [];
t_rebalance(num_partitions) ->
  10;
t_rebalance(_Config) when is_list(_Config) ->
  NPart = 10,
  PipeSpec = [ {map, kflow_test_map, #{id => '0'}}
             , {map, kflow_test_map, #{id => '1'}}
             ],
  Spec = fun(ID) ->
             #{ group_id  => ?group_id
              , topics    => [?topic]
              , id        => ID
              , pipe_spec => PipeSpec
              }
         end,
  ?check_trace(
     #{timeout => 1000},
     %% Run stage:
     begin
       %% Start first consumer, and let it spawn partition workers:
       {ok, PID1} = kflow_kafka_consumer_v2:start_link(Spec(a)),
       [begin
          produce({?topic, I}, <<1>>),
          ?block_until(#{ ?snk_kind := kflow_test_map_seen_message
                        , payload := #{partition := I, value := <<1>>}
                        })
        end || I <- lists:seq(0, NPart - 1)],
       %% Trigger rebalance:
       {ok, PID2} = kflow_kafka_consumer_v2:start_link(Spec(b)),
       timer:sleep(5000),
       kflow_kafka_consumer_v2:stop(PID1),
       kflow_kafka_consumer_v2:stop(PID2)
     end,
     %% Check stage:
     fun(_, Trace) ->
         kflow_trace_specs:init_terminate(Trace)
     end).

%% Fault-tolerance testcase for unfold behavior
t_unfold_crash({pipe_config, _}) ->
  %% Create a pipe that duplicates messages
  PipeSpec = [ {unfold, kflow_test_unfold, #{id => '1', replicate => 2}}
             , {map, fun(_, Msg) ->
                         ?maybe_crash(test_maybe_crash, #{}),
                         Msg
                     end}
             , {map, kflow_test_map, #{id => '2'}}
             ],
  %% TODO: Force brod to create message sets containing only one
  %% message, to let group_subscriber process offset commit requests;
  %% it's a workaround for a brod bug:
  %% https://github.com/klarna/brod/issues/361
  Args = #{ group_id        => ?group_id
          , topics          => [?topic]
          , id              => ?FUNCTION_NAME
          , pipe_spec       => PipeSpec
          , consumer_config => consumer_config()
          },
  [#{ start => {kflow_kafka_consumer_v2, start_link}
    , args  => Args
    }];
t_unfold_crash(num_partitions) ->
  1;
t_unfold_crash(_Config) when is_list(_Config) ->
  NMessages  = 30,
  MaxOffset  = NMessages - 1,
  Partitions = [0],
  Messages = [integer_to_binary(I) || I <- lists:seq(1, NMessages)],
  ?check_trace(
     %% Run stage:
     begin
       ?inject_crash( #{?snk_kind := test_maybe_crash}
                    , snabbkaffe_nemesis:periodic_crash(20, 0.9, 0)
                    ),
       [produce({?topic, 0}, I) || I <- Messages],
       {ok, _} = ?block_until(#{ ?snk_kind := kflow_kafka_commit_offset
                               , offset    := MaxOffset
                               }, 30000, infinity)
     end,
     %% Check stage:
     fun(_, Trace) ->
         %% Collect messages that reached the last node:
         Received = ?projection(payload, ?of_kind(kflow_test_map_seen_message, Trace)),
         %% Verify that all messages were received twice by the last
         %% node:
         lists:foreach(fun(Expected) ->
                           %% Verify that each payload is recieved at
                           %% least twice (since all messages are
                           %% duplicated by the unfold pipe):
                           ?assertMatch( [_, _|_]
                                       , [V || #{value := V} <- Received
                                             , V =:= Expected]
                                       )
                       end,
                       Messages)
     end).

t_healthcheck({pipe_config, _}) ->
  PipeSpec = [{map, fun(_, Msg) -> Msg end}],
  Args = #{ group_id        => ?group_id
          , topics          => [?topic]
          , id              => ?FUNCTION_NAME
          , pipe_spec       => PipeSpec
          , consumer_config => consumer_config()
          },
  [#{ start => {kflow_kafka_consumer_v2, start_link}
    , args  => Args
    }];
t_healthcheck(num_partitions) -> 1;
t_healthcheck(_Config) when is_list(_Config) ->
  application:ensure_all_started(hackney),
  ?assertMatch({ok, 200, _, _}, hackney:request(get, "localhost:8080/metrics")),
  ?assertMatch({ok, 200, _, _}, hackney:request(get, "localhost:8080/healthcheck")).

%%====================================================================
%% Trace validation functions
%%====================================================================

%% Check that each message produced to the topic ends up seen by a
%% specified pipe exactly once:
message_conservation(Trace, Id) ->
  ?strict_causality( #{ ?snk_kind := test_topic_produce
                      , partition := _P
                      , value     := _V
                      }
                   , #{ ?snk_kind := kflow_test_map_seen_message
                      , payload   := #{ value     := _V
                                      , partition := _P
                                      }
                      , id        := Id
                      }
                   , Trace
                   ).

%% Check that messages arrive in order:
message_ordering(Trace, Id, Partitions) ->
  lists:foreach( fun(P) ->
                     Messages = [Offset || #{ payload   := #{partition := P_}
                                            , offset    := Offset
                                            , id        := Id
                                            , ?snk_kind := kflow_test_map_seen_message
                                            } <- Trace, P_ =:= P],
                     snabbkaffe:strictly_increasing(Messages)
                 end
               , Partitions
               ).

%%====================================================================
%% Internal functions
%%====================================================================
