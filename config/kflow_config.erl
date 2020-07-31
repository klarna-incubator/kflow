%% This is a basic config for developerment
-module(kflow_config).

-export([pipes/0]).

pipes() ->
  [ sysmon_receiver()
  ].

sysmon_receiver() ->
  Pass = os:getenv("POSTGRES_KFLOW_PWD"),
  kflow_wf_sysmon_receiver:workflow( sysmon_receiver
                                   , #{ kafka_topic => <<"system_monitor">>
                                      , group_id    => <<"kflow_sysmon_consumer">>
                                      , database    => #{ host     => "localhost"
                                                        , username => "kflow"
                                                        , password => Pass
                                                        , database => "postgres"
                                                        }
                                      }
                                   ).
