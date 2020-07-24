%%%
%%%   Copyright (c) 2018-2020 Klarna Bank AB (publ)
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%
-module(kflow_http).

%% API
-export([init/0]).

-include_lib("hut/include/hut.hrl").

init() ->
  {ok, TransportOpts} = application:get_env(kflow, healthcheck_listener),
  Env = #{dispatch => dispatch()},
  ProtocolOpts = #{env => Env},
  CowboyStart =
    case application:get_env(kflow, healthcheck_tls) of
      {ok, true} ->
        ?log(info, "Starting HTTPS listener with parameters ~p", [ProtocolOpts]),
        fun cowboy:start_tls/3;
      {ok, false} ->
        ?log(info, "Starting HTTP listener with parameters ~p", [ProtocolOpts]),
        fun cowboy:start_clear/3
    end,
  case CowboyStart(http, TransportOpts, ProtocolOpts) of
    {ok, _Pid} ->
      ok;
    {error, {already_started, _Pid}} ->
      ok;
    Other ->
      {error, Other}
  end.

dispatch() ->
  cowboy_router:compile([{'_', routes()}]).

routes() ->
  [ {"/healthcheck", kflow_http_healthcheck_handler, []}
  , {"/metrics",     kflow_http_metrics_handler,     []}
  ].

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
