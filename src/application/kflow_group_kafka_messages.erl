%%%===================================================================
%%% @copyright 2018-2019 Klarna Bank AB (publ)
%%%
%%% @doc This module collects kafka messages into a list.
%%%
%%% <b>Input message format:</b>
%%% ```
%%% #{ key => K
%%%  , value => V
%%%  , headers => Headers
%%%  , partition => Partition
%%%  }
%%% '''
%%%
%%% <b>Output message format:</b>
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
%%% Partition of the first seen message is used for the entire chunk.
%%%
%%% == Example usage ==
%%%
%%% ```
%%% {aggregate, kflow_group_kafka_messages, #{ max_messages => 42
%%%                                          , max_size => 10000
%%%                                          }}'''
%%%
%%% @end
%%%===================================================================
-module(kflow_group_kafka_messages).

-behaviour(kflow_gen_aggregate).

%% gen_aggregate callbacks:
-export([in/4, out/2, init/1]).

-export_type([config/0]).

-define(META_FIELDS, [partition]).

-define(DEFAULT_MAX_MESSAGES, 100).

%%%===================================================================
%%% Types
%%%===================================================================

-type config() ::
        #{ max_messages => non_neg_integer()
         , max_size     => non_neg_integer() | undefined
         }.

-record(s,
        { acc = []       :: [map()]
        , n_messages = 0 :: non_neg_integer()
        , size = 0       :: non_neg_integer()
        , partition      :: non_neg_integer()
        }).

%%%===================================================================
%%% kflow_gen_aggregate callbacks
%%%===================================================================

%% @private
init(_Config) ->
  undefined.

%% @private
in(Offset, Msg = #{partition := P}, undefined, Config) ->
  %% Initialize state:
  S = #s{partition = P},
  in(Offset, Msg, S, Config);
in(_Offset, Msg0, State0, Config) ->
  Msg = strip(Msg0),
  MaxMessages = maps:get(max_messages, Config, ?DEFAULT_MAX_MESSAGES),
  MaxSize = maps:get(max_size, Config, undefined),
  #s{ acc        = Acc
    , n_messages = NMessages
    , size       = Size
    } = State0,
  %% Avoid expensive BIF when size doesn't matter:
  MsgSize = case MaxSize of
              undefined -> 0;
              _         -> erlang:external_size(term_to_binary(Msg))
            end,
  Flush = if NMessages >= MaxMessages;
             Size + MsgSize >= MaxSize -> flush;
             true                      -> keep
          end,
  State = State0 #s{ n_messages = NMessages + 1
                   , size       = Size + MsgSize
                   , acc        = [Msg|Acc]
                   },
  {Flush, State}.

%% @private
out(#s{acc = Acc, partition = P}, _Config) ->
  Msg = #{ values    => lists:reverse(Acc)
         , partition => P
         },
  {exit, Msg, undefined}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec strip(map()) -> map().
strip(Msg) ->
  maps:without(?META_FIELDS, Msg).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
