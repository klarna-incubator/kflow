%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This workflow assembles chunked transmissions into local
%%% files.
%%%
%%% @end
%%%===================================================================
-module(kflow_wf_chunks_to_local_file).

-include("kflow_int.hrl").

%% API
-export([workflow/2]).

-export_type([config/0, predicate/0]).

%%%===================================================================
%%% Types
%%%===================================================================

-type predicate() :: fun((Key :: binary()) -> boolean()).

-type config() ::
        #{ kafka_client    => atom()
         , consumer_config => proplists:proplist()
         , kafka_topic     := brod:topic()
         , group_id        := brod:group_id()
         , location        := file:filename_all()
         , filter          => predicate()
         }.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Create a workflow specification
-spec workflow(atom(), config()) -> kflow:workflow().
workflow(Id, Config) ->
  kflow:mk_kafka_workflow(Id, pipe_spec(Config), Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec pipe_spec(config()) -> kflow:pipe().
pipe_spec(Config) ->
  Preprocess = maps:get(preprocess, Config, []),
  Preprocess ++
    [ %% 1. Create a dedicated substream for each file:
      {demux, fun(_Offset, #{key := Key}) -> Key end}
      %% 2. Assemble chunks:
    , {assemble_chunks, kflow_chunks_to_file, Config}
    ].
