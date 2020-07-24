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
-module(kflow_http_healthcheck_handler).

-export([ init/2
        , init/3
        , handle_request/2
        , content_types_provided/2
        ]).

init(Req, Opts) ->
  {cowboy_rest, Req, Opts}.

init(_Transport, _Req, []) ->
  {upgrade, protocol, cowboy_rest}.

content_types_provided(Req, State) ->
  {[{<<"application/json">>, handle_request}], Req, State}.

handle_request(Req, State) ->
  Response = check_processes(),
  {jsone:encode(Response), Req, State}.

check_processes() ->
  Procs = supervisor:which_children(kflow_pipe_sup) ++
          supervisor3:which_children(kflow_sup2),
  lists:foldl( fun({Id, Child, _, _}, Acc) ->
                   Name = iolist_to_binary(io_lib:format("~p", [Id])),
                   {Healthy, Message} = healthy_child(Child),
                   HealthCheck0 = #{ <<"actionable">> => true
                                   , <<"name">>       => Name
                                   , <<"type">>       => <<"SELF">>
                                   , <<"healthy">>    => Healthy
                                   , <<"message">>    => Message
                                   },
                   {Key, HealthCheck} =
                     case Healthy of
                       true ->
                         {<<"healthy">>, HealthCheck0};
                       false ->
                         {<<"unhealthy">>, HealthCheck0#{<<"severity">> => <<"CRITICAL">>}}
                     end,
                   maps:update_with( Key
                                   , fun(Old) ->
                                         [HealthCheck|Old]
                                     end
                                   , Acc
                                   )
               end
             , #{<<"healthy">> => [], <<"unhealthy">> => []}
             , Procs
             ).

%% Brain-dead test checking that child isn't in cyclic restart
healthy_child(Child) ->
  if is_pid(Child) ->
    case erlang:process_info(Child, reductions) of
      {reductions, Reds} when Reds > 1000 ->
        {true, <<"">>};
      {reductions, _} ->
        {false, <<"Server is in cyclic restart">>};
      _ ->
        {false, <<"Server is dead">>}
    end;
   true ->
    {false, <<"Server is dead">>}
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
