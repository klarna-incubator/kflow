%%%===================================================================
%%% @copyright 2020 Klarna Bank AB (publ)
%%%
%%% @doc This behavior helps writing code that assembles data
%%% transferred in chunks. It is designed to handle common cases of
%%% upstream failure resulting in retransimission of chunks.
%%%
%%% It is assumed that each upstream message contains chunk number and
%%% number of chunks in some form.
%%%
%%% One has to create a callback module with
%%% `kflow_gen_assemble_chunks' behavior.
%%%
%%% `init' and `terminate' callbacks are similar to those in {@link
%%% kflow_gen_map} or {@link kflow_gen_filter} behaviors.
%%%
%%% == Consuming upstream messages ==
%%%
%%% `in' callback is invoked for each unique chunk of
%%% data. Parameters:
%%%
%%% <ol><li>Offset of a message</li>
%%%
%%% <li>Chunk number</li>
%%%
%%% <li>Total number of chunks</li>
%%%
%%% <li>Message itself</li>
%%%
%%% <li>State of the callback module. This state is created in `init'
%%% callback and can be mutated in the callbacks.</li>
%%%
%%% <li>Last argument is initial configuration (constant)</li></ol>
%%%
%%% Return value is the next state.
%%%
%%% == Producing messages downstream ==
%%%
%%% `out' callback is used to produce a message downstream. It is
%%% invoked after the last chunk has been processed. It takes two
%%% arguments: first one is current state of the callback module and
%%% the second one is initial configuration.
%%%
%%% It should output a tuple `{ok | exit, DownstreamMessage, NextState}'
%%%
%%% == Example ==
%%%
%%% ```
%%% -module(my_aggregate).
%%%
%%% -behavior(kflow_gen_assemble_chunks).
%%%
%%% -export([init/1, chunk_num/1, chunk_count/1, in/6, out/2, terminate/1]).
%%%
%%% chunk_num(#{chunk_num := N}) -> N.
%%%
%%% chunk_count(#{chunk_count := N}) -> N.
%%%
%%% init(_Config) ->
%%%   [].
%%%
%%% in(Offset, N, Nmax, #{value := Val}, State, Config) ->
%%%   [Val|State].
%%%
%%% out(State, _Config) ->
%%%   Output = lists:reverse(State),
%%%   {exit, Output, undefined}.
%%%
%%% terminate(_State) ->
%%%   ok.
%%%
%%% '''
%%% @end
%%%===================================================================
-module(kflow_gen_assemble_chunks).

-behavior(kflow_gen).

-callback init(_Config) -> _State.

-callback in( kflow:offset()
            , ChunkNum :: non_neg_integer()
            , ChunkCnt :: non_neg_integer()
            , _Msg
            , State
            , _Config
            ) -> State.

-callback out(State, _Config) -> {ok | exit, _Msg, State}.

-callback terminate(_State, _Config) -> _.

-callback chunk_num(_Msg) -> non_neg_integer().

-callback chunk_count(_Msg) -> non_neg_integer().

-optional_callbacks([init/1, terminate/2]).

-export([init/2, handle_message/3, handle_flush/2, terminate/2]).

%% @private
init(NodeId, Config) ->
  kflow_gen_aggregate:init(NodeId, {kflow_gen_assemble_chunks_impl, Config}).

%% @private
handle_message(Msg, State, Config) ->
  kflow_gen_aggregate:handle_message(Msg, State, Config).

%% @private
handle_flush(State, Config) ->
  kflow_gen_aggregate:handle_flush(State, Config).

%% @private
terminate(State, Config) ->
  kflow_gen_aggregate:terminate(State, Config).
