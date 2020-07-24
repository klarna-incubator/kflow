%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc `kflow_kafka_consumer_v2' is a source type that reads data
%%% from Kafka
%%% @end
%%%===================================================================
-module(kflow_kafka_consumer_v2).

-behavior(brod_group_subscriber_v2).

-include("kflow_int.hrl").
-include_lib("brod/include/brod.hrl").

%% API
-export([ start_link/1
        , stop/1
        ]).

%% brod_group_subscriber_v2 callbacks:
-export([ init/2
        , handle_message/2
        , terminate/2
        ]).

-export_type([config/0]).

%%%===================================================================
%%% Types
%%%===================================================================

-type config() ::
        #{ brod_client_id   => atom()
         , group_id         := brod:group_id()
         , topics           := [brod:topic()]
         , id               := atom()
         , pipe_spec        := kflow:pipe()
                             | fun((kflow:node_spec()) -> kflow:pipe())
         , auto_commit      => boolean()
         , consumer_config  => brod:consumer_config()
         , feed_timeout     => timeout()
         , shutdown_timeout => timeout()
         , flush_interval   => non_neg_integer() | undefined
         }.

-record(s,
        { pid        :: pid()
        , entrypoint :: kflow_pipe:entrypoint()
        }).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(config()) -> {ok, pid()}.
start_link(Config = #{ id       := Id
                     , group_id := GroupId
                     , topics   := Topics
                     }) ->
  kflow_lib:ensure_log(Id),
  %% Construct `brod_group_subscriber_v2' config:
  ClientId = maps:get(brod_client_id, Config, ?default_brod_client),
  GSConfig = #{ cb_module       => ?MODULE
              , message_type    => message_set
              , init_data       => init_data(Config)
              , client          => ClientId
              , group_id        => GroupId
              , topics          => Topics
              , consumer_config => consumer_config(Config)
              },
  brod:start_link_group_subscriber_v2(GSConfig).

-spec stop(pid()) -> ok.
stop(Pid) ->
  brod_group_subscriber_v2:stop(Pid).

%%%===================================================================
%%% brod_group_subscriber_v2 callbacks
%%%===================================================================

init(InitInfo, Config) ->
  #{ topic      := Topic
   , partition  := Partition
   , commit_fun := CommitFun
   } = InitInfo,
  #{ pipe_spec := PipeSpec0
   , id        := ParentId
   } = Config,
  Id = ParentId ++ [list_to_atom(integer_to_list(Partition))],
  ?set_process_metadata(#{domain => Id}),
  OffsetCommiterNode = {kflow_kafka_commit, undefined, InitInfo},
  PipeSpec = case maps:get(auto_commit, Config, true) of
               true ->
                 %% Commit offsets of fully processed messages:
                 PipeSpec0 ++ [OffsetCommiterNode];
               false when is_function(PipeSpec0, 1) ->
                 %% Let user insert OffsetCommitNode in any place:
                 PipeSpec0(OffsetCommiterNode);
               false ->
                 %% Offsets are not committed at all:
                 PipeSpec0
             end,
  %% Transform `Config' to `kflow_pipe:pipe_config()':
  PipeConfig0 = maps:with([feed_timeout, shutdown_timeout, flush_interval], Config),
  PipeConfig = PipeConfig0 #{ definition => PipeSpec
                            , id         => Id
                            },
  %% Start pipe:
  {ok, Pid, Entrypoint} = kflow_pipe:start_link(PipeConfig),
  ?tp(kflow_consumer_start, #{ topic     => Topic
                             , partition => Partition
                             , id        => Id
                             }),
  {ok, #s{ pid        = Pid
         , entrypoint = Entrypoint
         }}.

handle_message(MessageSet, State = #s{entrypoint = Entrypoint}) ->
  #kafka_message_set{ high_wm_offset = HighWm
                    , messages       = Messages
                    , topic          = Topic
                    , partition      = Partition
                    } = MessageSet,
  Route = [{Topic, Partition}],
  lists:foreach( fun(#kafka_message{ offset  = Offset
                                   , key     = Key
                                   , value   = Value
                                   , headers = Headers
                                   }) ->
                     Payload = #{ key       => Key
                                , value     => Value
                                , headers   => Headers
                                , partition => Partition
                                },
                     Msg = #kflow_msg{ offset  = Offset
                                     , route   = Route
                                     , payload = Payload
                                     },
                     ok = Entrypoint(Msg)
                 end
               , Messages
               ),
  {ok, ack, State}.

terminate(_Reason, #s{pid = PID}) ->
  kflow_pipe:stop(PID).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec consumer_config(config()) -> brod:consumer_config().
consumer_config(Config) ->
  ConsumerConfig = lists:keysort(1, maps:get(consumer_config, Config, [])),
  DefaultConsumerConfig = [{begin_offset, earliest}],
  lists:ukeymerge(1, ConsumerConfig, DefaultConsumerConfig).

-spec init_data(config()) -> kflow_pipe:pipe_config().
init_data(Config = #{id := Id}) ->
  %% Construct callback module config:
  NodeId = kflow_lib:root_node_id(Id),
  InitDataFields = [pipe_spec, auto_commit, feed_timeout, shutdown_timeout,
                    flush_interval],
  InitData0 = maps:with(InitDataFields, Config),
  InitData0 #{id => NodeId}.
