%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module contains operation and maintenance functions.
%%%
%%% @end
%%%===================================================================
-module(kflow).

-include("kflow_int.hrl").

%% API exports
-export([ mk_kafka_workflow/3
        , kafka_client_settings/1
        , kafka_client_settings/0
        , status/0
        ]).

-export_type([ message/0
             , node_id/0
             , node_spec/1
             , node_spec/0
             , node_config/0
             , behavior/0
             , offset/0
             , pipe/0
             , workflow/0
             , kafka_config/0
             ]).

%%====================================================================
%% Types
%%====================================================================

-type message() :: #kflow_msg{}.

-type node_id() :: [atom()].

-type offset() :: integer().

-type behavior() :: atom().

-type route() :: list().

-type node_config() :: #{ hard_timeout  => timeout()
                        , max_queue_len => non_neg_integer()
                        }.

-type node_spec(CallbackConfig) ::
        { Behavior       :: behavior()
        , CallbackModule :: module()
        , CallbackConfig
        }
      | { Behavior       :: behavior()
        , NodeConfig     :: node_config()
        , CallbackModule :: module()
        , CallbackConfig
        }
      | {map,             kflow_gen_map:callback_fun()}
      | {filter,          kflow_gen_filter:callback_fun()}
      | {demux,           kflow_gen_demux:callback_fun()}
      | {mfd,             kflow_gen_mfd:callback_fun()}
      | {unfold,          kflow_gen_unfold:callback_fun()}
      | {route_dependent, kflow_route_dependent:callback_fun()}
      | join
      .

-type node_spec() :: node_spec(_).

-type pipe() :: [node_spec(), ...].

-type workflow() ::
        #{ start := {module(), atom()}
         , args  := #{id := atom(), _ => _}
         , _     => _ %% TODO: Remove after migration to new brod behavior
         }.

-type kafka_config() ::
        #{ kafka_topic     := binary()
         , group_id        := binary()
         , kafka_client    => atom()
         , consumer_config => brod:consumer_config()
         , _               => _
         }.

%%====================================================================
%% API functions
%%====================================================================

%% @doc Get high-level health status. TODO: put something useful here
-spec status() -> string().
status() ->
  "kflow<br/>"
  "<font color=#0f0>UP</font></br>".

%% @doc Helper function for creating standard Kafka workflows
-spec mk_kafka_workflow(atom(), pipe(), kafka_config()) -> workflow().
mk_kafka_workflow(Id, PipeSpec, Config) ->
  #{ kafka_topic    := Topic
   , group_id       := GroupId
   } = Config,
  Client = maps:get(kafka_client, Config, ?default_brod_client),
  Args = (maps:with([feed_timeout, shutdown_timeout, flush_interval, auto_commit], Config))
           #{ group_id        => GroupId
            , topics          => [Topic]
            , id              => Id
            , pipe_spec       => PipeSpec
            , consumer_config => maps:get(consumer_config, Config, [])
            , brod_client_id  => Client
            },
  #{ start => {kflow_kafka_consumer_v2, start_link}
   , args  => Args
   }.

%% @doc Get Kafka connection setting of the default client
-spec kafka_client_settings() ->
                               {[brod:endpoint()], brod:client_config()}.
kafka_client_settings() ->
  kafka_client_settings(?default_brod_client).

%% @doc Get Kafka connection settings
-spec kafka_client_settings(atom()) ->
                               {[brod:endpoint()], brod:client_config()}.
kafka_client_settings(Name) ->
  Endpoints  = kafka_config(Name, kafka_endpoints),
  ReqTimeout = kafka_config(Name, kafka_request_timeout),
  SSL        = kafka_config(Name, kafka_ssl),
  SASL       = kafka_config(Name, kafka_sasl),
  DefaultProducerConfig = [{compression, gzip}],
  ClientConfig0 = [ {allow_topic_auto_creation, false}
                  , {compression,               gzip}
                  , {request_timeout,           ReqTimeout}
                  , {auto_start_producers,      true}
                  , {default_producer_config,   DefaultProducerConfig}
                  , {ssl,                       SSL}
                  ],
  {Endpoints,
   case SASL of
     true ->
       SaslFile = kafka_config(Name, kafka_sasl_file),
       ClientConfig0 ++ [{sasl, {plain, SaslFile}}];
     false ->
       ClientConfig0
   end}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Try to find configuration key of brod client, and fallback
%% to the default value.
-spec kafka_config(atom(), atom()) -> term().
kafka_config(ClientName, Key) ->
  case ?cfg(kafka_clients) of
    #{ClientName := #{Key := Override}} ->
      Override;
    _ ->
      ?cfg(Key)
  end.
