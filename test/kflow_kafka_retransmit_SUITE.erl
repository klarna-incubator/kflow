-module(kflow_kafka_retransmit_SUITE).

-include("kflow_int.hrl").
-include_lib("kflow/src/testbed/kafka_ct_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").

-define(downstream_topic(TestCase), list_to_binary("downstream_" ++ atom_to_list(TestCase))).
-define(downstream_topic, ?downstream_topic(?FUNCTION_NAME)).

-define(test_group_id(Topic), <<(Topic)/binary, "_test_consumer">>).

-define(downstream_n_part, 5).

-compile(export_all).

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_suite(Config) ->
  application:set_env(kflow, kafka_clients, #{ ?default_brod_client => #{}
                                             , downstream_client => #{}
                                             }),
  kflow_kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) ->
  ok.

suite() -> [{timetrap, {seconds, 250}}].

common_init_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config).

common_end_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_end_per_testcase(Case, Config).

%%====================================================================
%% Testcases
%%====================================================================

t_retransmit({pipe_config, _}) ->
  ok = create_topic(?downstream_topic, ?downstream_n_part),
  Config = #{ to_client       => downstream_client
            , n_partitions    => ?downstream_n_part
            , group_id        => ?group_id
            , to_topic        => ?downstream_topic
            , from_topic      => ?topic
            , consumer_config => consumer_config()
            },
  [kflow_kafka_retransmit:workflow(?MODULE, Config)];
t_retransmit(num_partitions) -> 1;
t_retransmit(Config) when is_list(Config) ->
  NMessages = 1000,
  Messages = lists:seq(1, NMessages),
  ?check_trace(
     begin
       %% Produce some messages to the upstream topic:
       [produce({?topic, 0}, integer_to_binary(I), integer_to_binary(I)) || I <- Messages],
       ?log(notice, "Done producing messages", []),
       %% Retry until the number of messages in the downstream topic
       %% becomes equal to the expected number:
       wait_n_messages(?group_id, NMessages),
       %% Collect retransmitted messages to the trace:
       [kflow_kafka_test_helper:dump_topic(?downstream_topic, I)
        || I <- lists:seq(0, ?downstream_n_part - 1)]
     end,
     fun(_Ret, Trace) ->
         %% Verify that messages are not lost:
         Received = ?of_kind(kafka_topic_message, Trace),
         Expected = [integer_to_binary(I) || I <- Messages],
         ?projection_complete(value, Received, Expected),
         %% Verify that messages are not reordered. Since we don't
         %% know how repartitioning works, we only check that messages
         %% with higher offsets have higher values:
         [begin
            %% Get all messages received in this parition:
            Msgs = [binary_to_integer(V)
                    || #{partition := P_, value := V} <- Received, P_ =:= P],
            %% Verify that values are ordered:
            snabbkaffe:strictly_increasing(Msgs)
          end || P <- lists:seq(0, ?downstream_n_part - 1)],
         true
     end).

%% NOTE: Troubleshooting this testcase if it breaks may be a bit
%% tricky. It's a good idea to first remove injected errors and ensure
%% topic at config stage of the testcase; and make sure it works like
%% this. Then gradually add more errors.
t_retransmit_crash({pipe_config, _}) ->
  %% Create kflow pipe config:
  Config = #{ to_client       => downstream_client
            , n_partitions    => ?downstream_n_part
            , group_id        => ?group_id
            , to_topic        => ?downstream_topic
            , from_topic      => ?topic
            , consumer_config => consumer_config()
            , max_messages    => 1
            },
  [kflow_kafka_retransmit:workflow(?MODULE, Config)];
t_retransmit_crash(num_partitions) -> 1;
t_retransmit_crash(Config) when is_list(Config) ->
  NMessages = 60,
  Messages = lists:seq(1, NMessages),
  UpstreamGroupId = ?group_id,
  DownstreamTopic = ?downstream_topic,
  ?check_trace(
     begin
       %% ...Emulate crashes in Kafka:
       ?inject_crash( #{ ?snk_kind := pre_kafka_message_send
                       , topic := DownstreamTopic
                       }
                    , snabbkaffe_nemesis:periodic_crash(10, 0.9, math:pi())
                    ),
       %% Produce some messages to the upstream topic:
       [produce({?topic, 0}, integer_to_binary(I), integer_to_binary(I)) || I <- Messages],
       ?log(notice, "Done producing messages", []),
       %% Create downstream topic _after_ starting the producer; with
       %% some luck it will crash it "the natural way". But we also
       %% injected some errors directly into the brod produce
       %% function, just for good measure.
       ok = create_topic(?downstream_topic, ?downstream_n_part),
       %% Wait until the workflow acks the last message in the
       %% upstream topic:
       wait_n_messages(UpstreamGroupId, NMessages, 1000),
       %% Collect retransmitted messages to the trace:
       [kflow_kafka_test_helper:dump_topic(DownstreamTopic, I)
        || I <- lists:seq(0, ?downstream_n_part - 1)]
     end,
     fun(_Ret, Trace) ->
         %% Verify that messages aren't lost:
         Received = ?of_kind(kafka_topic_message, Trace),
         Expected = [integer_to_binary(I) || I <- Messages],
         ?projection_complete(value, Received, Expected),
         NCrashes = length(?of_kind(snabbkaffe_crash, Trace)),
         ?log(notice, "Number of injected crashes: ~p", [NCrashes])
     end).

%%====================================================================
%% Trace validation functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================
