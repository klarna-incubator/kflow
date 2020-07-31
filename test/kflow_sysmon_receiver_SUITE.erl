-module(kflow_sysmon_receiver_SUITE).

-include("kflow_int.hrl").
-include_lib("kflow/src/testbed/kafka_ct_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").


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
  Config = #{ kafka_topic => ?topic
            , group_id    => ?group_id
            , database    => db_config()
            },
  [kflow_wf_sysmon_receiver:workflow(?MODULE, Config)];
t_retransmit(num_partitions) -> 3;
t_retransmit(Config) when is_list(Config) ->
  application:set_env(system_monitor, kafka_topic, ?topic),
  {ok, _} = application:ensure_all_started(system_monitor),
  timer:sleep(60000).

%%====================================================================
%% Internal functions
%%====================================================================

db_config() ->
  #{ host     => "localhost"
   , username => "kflow"
   , database => "postgres"
   , password => "123"
   }.
