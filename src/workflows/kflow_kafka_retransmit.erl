%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This workflow consumes messages from one topic and
%%% retransmits them to a different one. Partitions are chosen by
%%% hashing the Kafka message key
%%%
%%% @end
%%%===================================================================
-module(kflow_kafka_retransmit).

-behavior(kflow_gen_map).

-include("kflow_int.hrl").

%% API
-export([workflow/2]).

%% Callbacks
-export([map/3]).

-export_type([config/0]).

-define(out_part, partition).

%%%===================================================================
%%% Types
%%%===================================================================

-type part_fun() :: fun((NumPartitions :: non_neg_integer(), _) ->
                           brod:partition()).

-type config() ::
        #{ from_client    => atom()
         , to_client      => atom()
         , from_topic     := brod:topic()
         , to_topic       := brod:topic()
         , n_partitions   := integer()
         , group_id       := brod:group_id()
         , preprocess     => kflow:pipe()
         , part_fun       => part_fun()
         , max_messages   => non_neg_integer()
         , max_size       => non_neg_integer()
         , flush_interval => timeout()
         }.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Create a workflow specification
-spec workflow(atom(), config()) -> kflow:workflow().
workflow(Id, Config0) ->
  #{from_topic := FromTopic} = Config0,
  Config = (maps:without([from_topic], Config0))
             #{ kafka_topic    => FromTopic
              , flush_interval => maps:get(flush_interval, Config0, 5000)
              },
  kflow:mk_kafka_workflow(Id, pipe_spec(Config), Config).

%%%===================================================================
%%% kflow_gen_map callbacks
%%%===================================================================

%% @private
map(_Offset, Msg, {PartFun, NPartitions}) ->
  OutPartition = PartFun(NPartitions, Msg),
  Msg #{?out_part => OutPartition}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% This is how one implements Brucke(filter) via Kflow DSL:
-spec pipe_spec(map()) -> kflow:pipe().
pipe_spec(Config) ->
  #{ to_topic     := ToTopic
   , n_partitions := NPartitions
   } = Config,
  ToClient = maps:get(from_client, Config, ?default_brod_client),
  Preprocess = maps:get(preprocess, Config, []),
  PartFun = maps:get(part_fun, Config, fun partition_by_key/2),
  BufferConfig = maps:with([max_size, max_messages], Config),
  Preprocess ++
    [ %% First, choose what partition the message should end up in the downstream topic:
      {map, ?MODULE,
       {PartFun, NPartitions}}
      %% Then separate messages by partition:
    , {demux,
       fun(_Offset, #{?out_part := P}) -> P end}
      %% Group messages in chunks:
    , {aggregate, kflow_group_kafka_messages,
       BufferConfig}
      %% And finally push chunks to another topic:
    , {map, kflow_produce_to_kafka,
       #{ topic  => ToTopic
        , client => ToClient
        }}
    ].

-spec partition_by_key(non_neg_integer(), #{key := _}) -> brod:partition().
partition_by_key(NumPartitions, #{key := Key}) ->
  erlang:phash2(Key, NumPartitions).
