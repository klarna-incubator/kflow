%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%% @private
%%%===================================================================
-module(kflow_gen_assemble_chunks_impl).

-behavior(kflow_gen_aggregate).

-export([init/1, in/4, out/2, terminate/2]).

-include_lib("kflow/include/kflow_int.hrl").

-record(s,
        { bogus = false      :: boolean()
        , key                :: any()
        , last_chunk_num = 0 :: non_neg_integer()
        , n_chunks           :: non_neg_integer()
        , state              :: any()
        }).

-type state() :: #s{} | {initialized, term()}.

-spec init({module(), term()}) -> state().
init({Module, Config}) ->
  MS = kflow_lib:optional_callback(Module, init, [Config], undefined),
  {initialized, MS}.

-spec terminate(state(), {module(), term()}) -> _.
terminate(State, {Module, Config}) ->
  case State of
    #s{state = S}    -> ok;
    {initialized, S} -> ok
  end,
  kflow_lib:optional_callback(Module, terminate, [S, Config]).

in(Offset, Msg, {initialized, MS}, MC = {Module, _}) ->
  ChunkNum = Module:chunk_num(Msg),
  ChunkCnt = Module:chunk_count(Msg),
  Key = maps:get(key, Msg, undefined),
  if ChunkNum =:= 1 -> %% This is the beginning of a normal transfer:
      ?slog(info, #{ what      => "New transfer"
                   , key       => Key
                   , chunk_cnt => ChunkCnt
                   , offset    => Offset
                   }),
      State = #s{ n_chunks       = ChunkCnt
                , key            = Key
                , state          = MS
                , last_chunk_num = 0
                },
      handle_message(Offset, ChunkNum, ChunkCnt, Msg, State, MC);
     true -> %% This is the beginning of retransmission:
      ?slog(warning, #{ what      => "Ignoring retransmission"
                      , key       => Key
                      , slice_cnt => ChunkCnt
                      , slice_num => ChunkNum
                      , offset    => Offset
                      }),
      State = #s{ bogus          = true
                , key            = Key
                , n_chunks       = ChunkCnt
                , last_chunk_num = ChunkNum
                },
      handle_message(Offset, ChunkNum, ChunkCnt, Msg, State, MC)
  end;
in(Offset, Msg, State, MC = {Module, _}) ->
  %% This is continuation of a transfer:
  ChunkNum = Module:chunk_num(Msg),
  ChunkCnt = Module:chunk_count(Msg),
  handle_message(Offset, ChunkNum, ChunkCnt, Msg, State, MC).

out(#s{n_chunks = N, last_chunk_num = M}, _) when N =/= M ->
  %% Refuse to flush partially processed files:
  keep;
out(State = #s{bogus = true, key = Key}, _) ->
  Msg = #{ retransmission => true
         , key            => Key
         },
  {exit, Msg, State};
out(State0 = #s{state = MS0}, {Module, Config}) ->
  case Module:out(MS0, Config) of
    {Ret, Msg1, MS} when Ret =:= ok; Ret =:= exit ->
      Msg = Msg1#{retransmission => false},
      {Ret, Msg, State0#s{state = MS}};
    Ret ->
      error({badreturn, Ret})
  end.

-spec handle_message( kflow:offset()
                    , non_neg_integer()
                    , non_neg_integer()
                    , map()
                    , #s{}
                    , {module(), term()}
                    ) -> {flush | keep, #s{}}.
handle_message(_, ChunkNum, ChunkCnt, _, State = #s{bogus = true}, _) ->
  %% Handle retransmission:
  Flush = if ChunkNum =:= ChunkCnt -> flush;
             true                  -> keep
          end,
  {Flush, State};
handle_message(Offset, ChunkNum, ChunkCnt, Msg, State, {Module, Config}) ->
  %% Handle normal transfer:
  #s{ last_chunk_num = LCN
    , state          = MS0
    } = State,
  Flush = if ChunkNum =:= ChunkCnt -> flush;
             true                  -> keep
          end,
  Key = maps:get(key, Msg, undefined),
  if ChunkNum =< LCN ->
      ?slog(warning, #{ what            => "Retransmission; waiting for the next chunk"
                      , key             => Key
                      , chunk_num       => ChunkNum
                      , processed_slice => LCN
                      , offset          => Offset
                      }),
      {keep, State};
     true ->
      MS = Module:in(Offset, ChunkNum, ChunkCnt, Msg, MS0, Config),
      ChunkNum =:= LCN + 1 orelse
        ?slog(alert, #{ what            => "Missing chunk(s); transmission is corrupted!"
                      , key             => Key
                      , chunk_cnt       => ChunkCnt
                      , chunk_num       => ChunkNum
                      , processed_slice => LCN
                      , offset          => Offset
                      }),
      {Flush, State#s{ last_chunk_num = ChunkNum
                     , state          = MS
                     }}
  end.
