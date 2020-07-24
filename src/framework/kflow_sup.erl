%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%% @private
%%%===================================================================
-module(kflow_sup).

-behaviour(supervisor3).

%% API
-export([start_link/0]).

%% Supervisor3 callbacks
-export([init/1, post_init/1]).

-include("kflow_int.hrl").

-define(SERVER, ?MODULE).

-ifndef(TEST).
-define(COOLDOWN, 10).
-else.
-define(COOLDOWN, 1).
-endif.

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
  supervisor3:start_link({local, ?SERVER}, ?MODULE, level1).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(level1) ->
  SupFlags = { _SupStrategy  = one_for_one
             , _SupIntensity = 0
             , _SupPeriod    = 5
             },
  StartArgs = [{local, kflow_sup2}, ?MODULE, level2],
  Level2 = { _Id         = kflow_sup2
           , _Start      = {supervisor3, start_link, StartArgs}
           , _Restart    = {permanent, ?COOLDOWN}
           , _Shutdown   = infinity
           , _Type       = supervisor
           , _Module     = [kflow_sup]
           },
  {ok, {SupFlags, [Level2]}};
init(level2) ->
  SupFlags = { _SupStrategy  = one_for_all
             , _SupIntensity = 1
             , _SupPeriod    = 5
             },
  BrodClients = brod_client_specs(),
  PipeSup = { _Id         = kflow_pipe_sup
            , _Start      = {kflow_pipe_sup, start_link, []}
            , _Restart    = permanent
            , _Shutdown   = infinity
            , _Type       = supervisor
            , _Module     = [kflow_pipe_sup]
            },
  {ok, {SupFlags, BrodClients ++ [PipeSup]}}.

post_init(_) ->
  ignore.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec brod_client_specs() -> [supervisor3:child_spec()].
brod_client_specs() ->
  Clients = ?cfg(kafka_clients),
  [brod_client_spec(I) || I <- maps:keys(Clients)].

%% @private Create a child spec for a brod client. The idea is that
%% bringing brod clients under kflow supervision tree will make
%% restarts more aggressive and more prone to recover stuck consumers
-spec brod_client_spec(atom()) -> supervisor3:child_spec().
brod_client_spec(Name) ->
  {Endpoints, ClientConfig} = kflow:kafka_client_settings(Name),
  InitArgs = [Endpoints, Name, ClientConfig],
  { _Id       = Name
  , _Start    = {brod_client, start_link, InitArgs}
  , _Restart  = {permanent, 10}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.
