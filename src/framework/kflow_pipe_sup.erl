%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%% @private
%%%===================================================================
-module(kflow_pipe_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("kflow_int.hrl").

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
  SupFlags = #{ strategy  => one_for_one
              , intensity => 1
              , period    => 5
              },
  Workflows = case ?cfg(pipes) of
                undefined ->
                  [];
                {Module, Function, Args} ->
                  apply(Module, Function, Args)
              end,
  Children = [child_spec(I, N) || I <- Workflows
                                %% TODO: Remove after migration to new brod group subscriber:
                                , N <- lists:seq( 1
                                                , maps:get(num_workers, I, 1)
                                                )],
  {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec child_spec(kflow:workflow(), integer()) -> supervisor:child_spec().
child_spec(#{start := {Module, Function}, args := Args}, Worker) ->
  #{id := NodeId} = Args,
  %% TODO: Remove after migration to new brod group subscriber:
  ID2 = list_to_atom(atom_to_list(NodeId) ++ integer_to_list(Worker)),
  #{ id       => ID2
   , start    => {Module, Function, [Args]}
   , restart  => permanent
   , shutdown => 35000
   , type     => worker
   }.
