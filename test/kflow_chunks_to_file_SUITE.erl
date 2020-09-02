-module(kflow_chunks_to_file_SUITE).

-include("kflow_int.hrl").
-include_lib("kflow/src/testbed/kafka_ct_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(test_group_id(Topic), <<(Topic)/binary, "_test_consumer">>).

-compile(export_all).

%%====================================================================
%% CT boilerplate
%%====================================================================

groups() ->
  [{s3, [t_write, t_delete]}].

init_per_suite(Config) ->
  %% Create parent directory as a non-privileged user to prevent
  %% docker from doing it as root:
  file:make_dir(location()),
  %% Set location for minio container:
  os:putenv("LOCATION", location() ++ "/s3"),
  kflow_kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) ->
  ok.

init_per_group(s3, Config) ->
  [{workflow, kflow_wf_chunks_to_s3} | Config];
init_per_group(_, Config) ->
  Config.

end_per_group(_Group, _Config) ->
  ok.

suite() -> [{timetrap, {seconds, 300}}].

common_init_per_testcase(Case, Config) ->
  ok = application:set_env(kflow, s3_options, []),
  ok = application:set_env(kflow, s3_headers, []),
  meck:new(kflow_utils, [no_history, passthrough]),
  ok = meck:expect(kflow_utils, aws_config,
                   fun() ->
                       #aws_config{ s3_port = 9000
                                  , s3_host = "localhost"
                                  , s3_scheme = "http://"
                                  , access_key_id = "alice"
                                  , s3_bucket_access_method = path
                                  , secret_access_key = "12345678"
                                  }
                   end),
  kflow_kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config).

common_end_per_testcase(Case, Config) ->
  meck:unload(kflow_utils),
  kflow_kafka_test_helper:common_end_per_testcase(Case, Config).

%%====================================================================
%% Testcases
%%====================================================================

t_write({pipe_config, TestConfig}) ->
  make_config(TestConfig, ?topic, ?group_id);
t_write(num_partitions) -> 1;
t_write(Config) when is_list(Config) ->
  ?log(notice, "Files will be found in: ~p", [location(Config)]),
  ChunkSize = 1 bsl 23,
  KFiles = [ {integer_to_binary(I), {100, ChunkSize * 10}}
           || I <- lists:seq(1, 10)],
  ?check_trace(
     %% Run stage:
     begin
       %% Create an interleaved stream of all kfiles:
       Streams = [{Key, payload_gen:generate_chunk(Data, ChunkSize)} ||
                   {Key, Data} <- KFiles],
       Stream0 = payload_gen:interleave_streams(Streams),
       Stream = payload_gen:retransmits(Stream0, 0.1),
       %% Dump it to Kafka topic and wait until Kflow processes it
       LastOffset = lists:max(binaries_to_kafka(?FUNCTION_NAME, Stream)),
       wait_n_messages(?group_id, LastOffset)
     end,
     %% Check stage:
     fun(_Result, _Trace) ->
         lists:foreach( fun({Name, Data}) ->
                            Path = filename:join(location(Config), Name),
                            payload_gen:check_file_consistency(Data, 1000, Path)
                        end
                      , KFiles
                      )
     end).

t_delete({pipe_config, TestConfig}) ->
  make_config(TestConfig, ?topic, ?group_id);
t_delete(num_partitions) -> 1;
t_delete(Config) when is_list(Config) ->
  ?log(notice, "Files will be found in: ~p", [location(Config)]),
  ChunkSize = 1 bsl 23,
  KFiles = [ {integer_to_binary(I), {100, ChunkSize * 10}}
           || I <- lists:seq(1, 10)],
  ?check_trace(
     %% Run stage:
     begin
       %% Create an interleaved stream of all kfiles:
       Streams = [{Key, payload_gen:generate_chunk(Data, ChunkSize)} ||
                   {Key, Data} <- KFiles],
       Stream = payload_gen:interleave_streams(Streams),
       %% Dump it to Kafka topic...
       LastOffset = lists:max(binaries_to_kafka(?FUNCTION_NAME, Stream)),
       wait_n_messages(?group_id, LastOffset),
       %% Check if files exist:
       lists:foreach( fun({Name, _Data}) ->
                          Path = filename:join(location(Config), Name),
                          ?assertMatch(true, filelib:is_file(Path))
                      end
                    , KFiles
                    ),
       %% Then send delete command:
       LastOffset2 = lists:last([delete_via_kafka(?topic, File)
                                 || {File, _} <- KFiles]),
       wait_n_messages(?group_id, LastOffset2)
     end,
     %% Check stage:
     fun(_Result, _Trace) ->
         lists:foreach( fun({Name, _Data}) ->
                            Path = filename:join(location(Config), Name),
                            ?assertMatch(false, filelib:is_file(Path))
                        end
                      , KFiles
                      )
     end).

%%====================================================================
%% Trace validation functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================

make_config(TestConfig, Topic, Group) ->
  case proplists:get_value(workflow, TestConfig, kflow_wf_chunks_to_local_file) of
    kflow_wf_chunks_to_local_file ->
      Config = #{ kafka_topic     => Topic
                , group_id        => Group
                , location        => location() ++ "/local"
                , consumer_config => consumer_config()
                },
      [kflow_wf_chunks_to_local_file:workflow(chunks_to_local_file, Config)];
    kflow_wf_chunks_to_s3 ->
      Config = #{ kafka_topic     => Topic
                , group_id        => Group
                , s3_bucket       => "bucket1"
                , consumer_config => consumer_config()
                , prefix          => "prefix"
                },
      [kflow_wf_chunks_to_s3:workflow(chunks_to_s3, Config)]
  end.

binaries_to_kafka(Testcase, Generator) ->
  Fun = fun({Key, {Value, ChunkNum, ChunkCnt}}) ->
            Headers = [ {<<"slice_num">>, integer_to_binary(ChunkNum)}
                      , {<<"slice_cnt">>, integer_to_binary(ChunkCnt)}
                      ],
            kflow_kafka_test_helper:produce({?topic(Testcase), 0}, Key, Value, Headers)
        end,
  payload_gen:consume(Generator, Fun).

delete_via_kafka(Topic, File) ->
  Headers = [ {<<"slice_num">>, <<"1">>}
            , {<<"slice_cnt">>, <<"1">>}
            , {<<"deleted">>, <<"1">>}
            ],
  kflow_kafka_test_helper:produce({Topic, 0}, File, <<>>, Headers).

location() ->
  {ok, CWD} = file:get_cwd(),
  CWD ++ "/files".

location(Config) ->
  case proplists:get_value(workflow, Config, kflow_wf_chunks_to_local_file) of
    kflow_wf_chunks_to_local_file ->
      location() ++ "/local";
    kflow_wf_chunks_to_s3 ->
      location() ++ "/s3/bucket1/prefix"
  end.
