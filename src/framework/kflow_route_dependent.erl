%%%===================================================================
%%% @copyright 2020 Klarna Bank AB (publ)
%%%
%%% @doc This wrapper module allows to make a regular <b>stateless</b>
%%% stream processing node dependent on the route (head element of the
%%% route, to be precise). NOTE: This module will intentionally crash
%%% when used with a stateful callback module.
%%%
%%% == Example ==
%%%
%%% The following example illustrates how to patch config of
%%% `kflow_postgres' table depending on the route:
%%%
%%% ```
%%% {demux, fun(_Offset, [#{value := N} | _]) ->
%%%           if N rem 2 =:= 0 -> even;
%%%              true ->          odd
%%%           end
%%%         end},
%%% {route_dependent,
%%%    fun(Route) ->
%%%      Table = case Route of
%%%                odd  -> "odds";
%%%                even -> "evens"
%%%              end,
%%%      {map, kflow_postgres, #{ database => #{host => "localhost"}
%%%                             , table    => Table
%%%                             }}
%%%    end}
%%% '''
%%%
%%% @end
-module(kflow_route_dependent).

-behavior(kflow_gen).

-export([init/2, handle_message/3, handle_flush/2, terminate/2]).

-export_type([callback_fun/0]).

-type callback_fun() :: fun((_RouteHd :: term()) -> kflow_pipe:node_spec()).

%% @private
-spec init(kflow:node_id(), callback_fun()) -> {ok, kflow_multistate:wrapped()}.
init(NodeId, Cfg) ->
  kflow_multistate:wrap(NodeId, kflow_route_dependent_impl, Cfg).

%% @private
handle_message(Msg, State, Config) ->
  kflow_multistate:handle_message(Msg, State, Config).

%% @private
handle_flush(State, Config) ->
  kflow_multistate:handle_flush(State, Config).

%% @private
terminate(State, Config) ->
  kflow_multistate:terminate(State, Config).
