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
-module(kflow_http_metrics_handler).

-export([ init/2
        ]).

init(Req0, State) ->
  Req = cowboy_req:reply( 200
                        , #{ <<"content-type">> =>
                               <<"text/plain; version=0.0.4">>
                           }
                        , prometheus_text_format:format()
                        , Req0
                        ),
  {ok, Req, State}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
