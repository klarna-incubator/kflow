-module(kflow_kafka_test_helper).

-export([ init_per_suite/1
        , common_init_per_testcase/3
        , common_end_per_testcase/2
        , produce/2
        , produce/3
        , produce/4
        , create_topic/2
        , get_acked_offsets/1
        , check_committed_offsets/2
        , wait_n_messages/3
        , wait_n_messages/2
        , wait_for_port/2
        , consumer_config/0
        , dump_topic/2
        ]).

-include("kflow_test_macros.hrl").

init_per_suite(Config) ->
  snabbkaffe:fix_ct_logging(),
  {ok, CWD} = file:get_cwd(),
  LogDir = filename:join(CWD, "kflow_logs"),
  application:load(kflow),
  ?log(notice, "Logs will be found in ~p~n", [LogDir]),
  application:set_env(kflow, log_dir, LogDir),
  application:set_env(kflow, pipe_log_level, debug),
  application:set_env(kflow, kafka_endpoints, [{?KAFKA_HOST, ?KAFKA_PORT}]),
  application:set_env(kflow, kafka_ssl, false),
  application:set_env(kflow, kafka_sasl, false),
  application:set_env(kflow, config_module_dir, undefined),
  {ok, _} = application:ensure_all_started(brod),
  [ {proper_timeout, 10000}
  , {ct_hooks, [docker_compose_cth]}
  | Config].

common_init_per_testcase(Module, Case, Config) ->
  %% Create a client and a producer for putting test data to Kafka.
  %% Name of the test topic is equal to the name of testcase
  ClientId       = ?test_client,
  BootstrapHosts = [{?KAFKA_HOST, ?KAFKA_PORT}],
  Topic          = ?topic(Case),
  NumPartitions  = try Module:Case(num_partitions) of
                       N -> N
                   catch
                     error:function_clause -> 1
                   end,
  ok = create_topic(Topic, NumPartitions),
  ok = brod:start_client(BootstrapHosts, ClientId, []),
  ?retry(_PollInterval = 1000, _NRetries = 10,
         begin
           ok = brod:start_producer(ClientId, Topic, _ProducerConfig = [])
         end),
  application:set_env(kflow, pipes, {Module, Case, [{pipe_config, Config}]}),
  application:ensure_all_started(kflow),
  Config.

common_end_per_testcase(_Case, _Config) ->
  ok = brod:stop_client(?test_client),
  application:stop(kflow).

produce(TopicPartition, Value) ->
  produce(TopicPartition, <<>>, Value).

produce(TopicPartition, Key, Value) ->
  produce(TopicPartition, Key, Value, []).

produce({Topic, Partition}, Key, Value, Headers) ->
  ?tp(test_topic_produce, #{ topic     => Topic
                           , partition => Partition
                           , key       => Key
                           , value     => Value
                           , headers   => Headers
                           }),
  {ok, Offset} = brod:produce_sync_offset( ?test_client, Topic, Partition, <<>>
                                         , [#{value => Value, key => Key, headers => Headers}]
                                         ),
  Offset.

create_topic(Name, NumPartitions) ->
  ?log(info, "Creating Kafka topic: ~p", [Name]),
  ConfigEntries = [ {config_name, "max.message.bytes"}
                  , {config_value, "20485760"} % ~20 MB
                  ],
  TopicFields = [ {topic, Name}
                , {num_partitions, NumPartitions}
                , {replication_factor, 1}
                , {replica_assignment, []}
                , {config_entries, [ConfigEntries]}
                ],
  Req = kpro_req_lib:create_topics( 0
                                  , [TopicFields]
                                  , #{timeout => 1000}
                                  ),
  ?retry(_PollInterval = 2000, _NRetries = 120,
         begin
           {ok, Conn} = kpro:connect_any([{?KAFKA_HOST, ?KAFKA_PORT}], []),
           try
             {ok, Result} = kpro:request_sync(Conn, Req, 1000),
             ?log(info, "KPRO response: ~p", [Result]),
             #{topic_errors := [#{error_code := TopicError}]} = Result#kpro_rsp.msg,
             case TopicError of
               no_error -> ok;
               topic_already_exists -> ok
             end
           after
             kpro:close_connection(Conn)
           end
         end),
  ok.

-spec get_acked_offsets(brod:group_id()) -> #{brod:partition() => brod:offset()}.
get_acked_offsets(GroupId) ->
  {ok, [#{partition_responses := Resp}]} =
    brod:fetch_committed_offsets(?test_client, GroupId),
  Fun = fun(#{partition := P, offset := O}, Acc) ->
            Acc #{P => O}
        end,
  lists:foldl(Fun, #{}, Resp).


%% Validate offsets committed to Kafka:
check_committed_offsets(GroupId, Offsets) ->
  CommittedOffsets = get_acked_offsets(GroupId),
  lists:foreach( fun({TopicPartition, Offset}) ->
                     %% Explanation for + 1: brod's `roundrobin_v2'
                     %% protocol keeps _first unprocessed_ offset
                     %% rather than _last processed_. And this is
                     %% confusing.
                     ?assertEqual( Offset + 1
                                 , maps:get(TopicPartition, CommittedOffsets, undefined)
                                 )
                 end
               , Offsets
               ).

%% Wait until total number of messages processed by a consumer group
%% becomes equal to the expected value
wait_n_messages(TestGroupId, Expected, NRetries) ->
  ?retry(1000, NRetries,
         begin
           Offsets = kflow_kafka_test_helper:get_acked_offsets(TestGroupId),
           NMessages = lists:sum(maps:values(Offsets)),
           ?log( notice
               , "Number of messages processed by consumer group: ~p; total: ~p/~p"
               , [Offsets, NMessages, Expected]
               ),
           ?assert(NMessages >= Expected)
         end).

wait_n_messages(TestGroupId, NMessages) ->
  wait_n_messages(TestGroupId, NMessages, 30).

-spec wait_for_port(string(), non_neg_integer()) -> ok.
wait_for_port(Host, Port) ->
  ?retry(1000, 10,
         begin
           {ok, Sock} = gen_tcp:connect(Host, Port, []),
           gen_tcp:close(Sock),
           ok
         end).

consumer_config() ->
  %% Makes brod restart faster, this will hopefully shave off some
  %% time from failure suites:
  [ {max_wait_time, 500}
  , {sleep_timeout, 100}
  , {max_bytes, 1}
  ].

%% Dump all messages in the topic to the snabbkaffe trace
dump_topic(Topic, Partition) ->
  FoldFun = fun(Msg, Acc) ->
                #kafka_message{offset = O, key = K, value = V} = Msg,
                ?tp(kafka_topic_message, #{ offset    => O
                                          , key       => K
                                          , value     => V
                                          , partition => Partition
                                          , topic     => Topic
                                          }),
                {ok, Acc}
            end,
  FetchOpts = #{},
  brod:fold( ?test_client, Topic, Partition, _BeginOffset = 0
           , FetchOpts, 0, FoldFun, _FoldLimits = #{}).
