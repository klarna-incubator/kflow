%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module implements a stateful stream processing node for
%%% many-into-one transformations.
%%%
%%% One has to create a callback module with `kflow_gen_aggregate'
%%% behavior.
%%%
%%% `init' and `terminate' callbacks are similar to those in {@link
%%% kflow_gen_map} or {@link kflow_gen_filter} behaviors.
%%%
%%% == Consuming upstream messages ==
%%%
%%% `in' callback is invoked for each incoming message from the
%%% upstream. It takes 4 arguments:
%%%
%%% <ol><li>Offset of a message</li>
%%%
%%% <li>Message itself</li>
%%%
%%% <li>State of the callback module. This state is created in `init'
%%% callback and can be mutated in the callbacks.</li>
%%%
%%% <li>Last argument is initial configuration of the aggregator
%%% (constant)</li></ol>
%%%
%%% Return value should be a tuple `{Flush, NextState}' where `Flush'
%%% can be atoms `keep', `flush' or `reject'.
%%%
%%% `keep' means that the aggregator should keep collecting upstream
%%% messages without producing anything downstream.
%%%
%%% `flush' means that the aggregator is ready to produce a message
%%% downstream.
%%%
%%% `reject' means the last upstream message was incompatible with the
%%% data that had been aggregated so far. (E.g. schema of the data was
%%% different). In this case <i>previous</i> state is flushed and the
%%% last message is replayed from blank state.
%%%
%%% == Producing messages downstream ==
%%%
%%% `out' callback is used to produce a message downstream. It is
%%% invoked when `in' callback returns `flush' or `reject', or when
%%% flush is implicitly requested by low-level control logic. It takes
%%% two arguments: first one is current state of the callback module
%%% and the second one is initial configuration.
%%%
%%% It should output a tuple `{ok | exit, DownstreamMessage, NextState}'
%%% or an atom `keep'.
%%%
%%% Returning `{ok, Msg, NextState}' will result in sending `Msg'
%%% downstream, and waiting for new messages with state `NextState'.
%%%
%%% Returning `{exit, Msg, NextState}' will result in sending `Msg'
%%% downstream, calling `terminate' callback, if it is defined by the
%%% user CBM, and then forgetting about the state of the user CBM for
%%% the route. This is useful when the number of routes is unlimited.
%%%
%%% If user CBM returns `keep', then gen_aggregate will keep the state
%%% and won't produce anything downstream. This is useful to avoid
%%% situation when `flush' is requested by some external logic, but
%%% user CBM doesn't want to to flush half-finished data.
%%%
%%% == Example ==
%%% ```
%%% -module(my_aggregate).
%%%
%%% -behavior(kflow_gen_aggregate).
%%%
%%% -export([init/1, in/4, out/2, terminate/1]).
%%%
%%% init(_Config) ->
%%%   [].
%%%
%%% in(Offset, Message, State, Config) ->
%%%   N = maps:get(buffer_size, Config),
%%%   Flush = if length(State) >= N ->
%%%                flush;
%%%              true ->
%%%                keep
%%%           end,
%%%   {Flush, [Message|State]}.
%%%
%%% out(State, _Config) ->
%%%   Output = lists:reverse(State),
%%%   NewState = [],
%%%   {ok, Output, NewState}.
%%%
%%% terminate(_State) ->
%%%   ok.
%%%
%%% '''
%%% @end
%%%===================================================================
-module(kflow_gen_aggregate).

-behavior(kflow_gen).

-include("kflow_int.hrl").

-export([init/2, handle_message/3, handle_flush/2, terminate/2]).

-type flush() :: keep    % Keep accumulating the messages
               | flush   % Flush the accumulated messages
               | reject. % Flush the messages accumulated before the last
                         % one and replay it with an empty buffer

-callback init(_Config) -> _State.

-callback in(kflow:offset(), _Msg, State, _Config) -> {flush(), State}.

-callback out(State, _Config) -> {ok | exit, _Msg, State}
                               | keep
                               .

-callback terminate(_State, _Config) -> _.

-optional_callbacks([init/1, terminate/2]).

%% @private
init(NodeId, Config) ->
  kflow_multistate:wrap(NodeId, kflow_gen_aggregate_impl, Config).

%% @private
handle_message(Msg, State, Config) ->
  kflow_multistate:handle_message(Msg, State, Config).

%% @private
handle_flush(State, Config) ->
  kflow_multistate:handle_flush(State, Config).

%% @private
terminate(State, Config) ->
  kflow_multistate:terminate(State, Config).
