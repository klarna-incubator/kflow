-module(kflow_config_load_tests).

-include_lib("eunit/include/eunit.hrl").

load_config_test() ->
  application:load(kflow),
  Dir = filename:join(code:lib_dir(kflow), "test/pipe_config"),
  io:format(user, "Kflow config dir: ~p", [Dir]),
  application:set_env(kflow, config_module_dir, Dir),
  %% Don't start any brod processes:
  application:set_env(kflow, kafka_clients, #{}),
  %% Do it 3 times to ensure that code is purged properly
  start_and_check(),
  start_and_check(),
  start_and_check().

start_and_check() ->
  ?assertMatch({ok, _}, application:ensure_all_started(kflow)),
  ?assertMatch( [#{start := {kflow_dummy_source, start_link}, args := _}]
              , kflow_config:pipes()
              ),
  application:stop(kflow).
