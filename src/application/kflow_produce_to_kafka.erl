%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module publishes message sets to a Kafka
%%% topic. `partition' key determines what partition the message set
%%% will be produced to.
%%%
%%% <b>Input message format:</b>
%%% ```
%%% #{ values => [ #{ key => K1
%%%                 , value => V1
%%%                 , headers => H1
%%%                 }
%%%              , #{ key => K2
%%%                 , value => V2
%%%                 , headers => H2
%%%                 }
%%%              , ...
%%%              ]
%%%  , partition => Partition
%%%  }
%%% '''
%%%
%%% <b>Output message format:</b>
%%%
%%% Messages are returned as is.
%%%
%%% == Example usage ==
%%%
%%% ```
%%%  {map, kflow_produce_to_kafka, #{ topic  => <<"downstream_topic">> % Mandatory
%%%                                 , client => ?kflow_default_client
%%%                                 }}
%%% '''
%%% @end
%%%===================================================================
-module(kflow_produce_to_kafka).

-behavior(kflow_gen_map).

-include("kflow_int.hrl").

-export([init/1, map/3, terminate/1]).

-export_type([config/0]).

%%%===================================================================
%%% Types
%%%===================================================================

-type config() ::
        #{ topic         := brod:topic()
         , client        => atom()
         }.

-record(s,
        { client :: atom()
        , topic  :: brod:topic()
        }).

%%%===================================================================
%%% kflow_gen_map callbacks
%%%===================================================================

%% @private
init(Config = #{topic := Topic}) ->
  prometheus_gauge:declare([ {name, <<"kflow_kafka_producer_offset">>}
                           , {help, "Offset of the last message produced to the topic"}
                           , {labels, [topic, partition]}
                           ]),
  prometheus_histogram:declare([ {name, <<"kflow_kafka_producer_time">>}
                               , {help, "Time spent to pushing messages to Kafka"}
                               , {labels, [topic, partition]}
                               , {buckets, [10, 100, 1000, 10000, 30000]}
                               ]),
  Client = maps:get(client, Config, ?default_brod_client),
  RequiredAcks = maps:get(required_acks, Config, -1),
  Timeout = maps:get(timeout, Config, 10000),
  Retries = maps:get(retries, Config, 10),
  ProducerConfig = [ {required_acks, RequiredAcks}
                   , {ack_timeout, Timeout}
                   , {max_retries, Retries}
                   ],
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  #s{ client = Client
    , topic  = Topic
    }.

%% @private
map(Offset, Msg = #{partition := P, value := Val}, State) ->
  #s{ client = Client
    , topic  = Topic
    } = State,
  TBefore = erlang:monotonic_time(millisecond),
  ?tp(info, pre_kafka_message_send,
      #{ topic     => Topic
       , partition => P
       , offset    => Offset
       }),
  case brod:produce_sync_offset(Client, Topic, P, <<>>, Val) of
    {ok, OutOffset} ->
      prometheus_gauge:set( <<"kflow_kafka_producer_offset">>
                          , [Topic, P]
                          , OutOffset
                          ),
      TAfter = erlang:monotonic_time(millisecond),
      prometheus_histogram:observe( <<"kflow_kafka_producer_time">>
                                  , [Topic, P]
                                  , TAfter - TBefore
                                  ),
      ?tp(info, kafka_message_sent,
          #{ topic     => Topic
           , partition => P
           , offset    => Offset
           }),
      Msg;
    Err ->
      ?slog(critical, #{ what         => "Brod produce failed"
                       , topic        => Topic
                       , partition    => P
                       , input_offset => Offset
                       , reason       => Err
                       , client       => Client
                       }),
      error(kafka_error)
  end.

%% @private
terminate(_State) ->
  void.
