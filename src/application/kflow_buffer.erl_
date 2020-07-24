%%%===================================================================
%%% @copyright 2018-2019 Klarna Bank AB (publ)
%%%
%%% @doc This module collects messages into a list, or concatenates
%%% lists of messages.
%%%
%%% <b>Input message format:</b>
%%% ```
%%% Msg \ [Msg]
%%% '''
%%%
%%% <b>Output message format:</b>
%%% ```
%%% [Msg]
%%% '''
%%%
%%% == Configuration ==
%%%
%%% The following optional parameters can be configured:
%%%
%%% === max_messages ===
%%%
%%% Type: `non_neg_integer()'. Buffer will be flushed when it collects
%%% this many messages. By default equals 100.
%%%
%%% === max_size ===
%%%
%%% Type: `non_neg_integer() | undefined'. Buffer will be flushed when
%%% it collects this many bytes of data. By default this is unlimited.
%%%
%%% == Example usage ==
%%%
%%% ```
%%% {aggregate, kflow_buffer, #{}}'''
%%%
%%% @end
%%%===================================================================
-module(kflow_buffer).

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
        }).

%%%===================================================================
%%% kflow_gen_aggregate callbacks
%%%===================================================================

%% @private
init(_Config) ->
  undefined.

%% @private
in(_Offset, Msg, undefined, _Config) ->
  S = #s{ acc        = [Msg]
        , n_messages = 1
        },
  {keep, S};
in(_Offset, Msg, State0, Config) ->
  MaxMessages = maps:get(max_messages, Config, ?DEFAULT_MAX_MESSAGES),
  MaxSize     = maps:get(max_size, Config, undefined),
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
out(#s{acc = Acc}, _Config) ->
  Msg = lists:reverse(Acc),
  {exit, Msg, undefined}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
