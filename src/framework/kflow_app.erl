%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @private This module reads kflow pipe configuration
%%% @end
%%%===================================================================
-module(kflow_app).

-behaviour(application).

-include("kflow_int.hrl").

-ifndef(TEST).
-define(TEST, false).
-endif.

%% Application callbacks
-export([start/2, stop/1]).

%%=============================================================================
%% API
%%=============================================================================

start(_StartType, _StartArgs) ->
  %% Avoid writing duplicate messages to the default log (unless in
  %% test build, where we want all logs in one place):
  Filter = {fun logger_filters:domain/2, {stop, sub, [kflow_pipe]}},
  ?TEST orelse logger:add_handler_filter(default, no_kflow_pipe, Filter),
  %% Start healthcheck listener:
  ok = kflow_http:init(),
  %% Load pipe configuration:
  case load_pipe_config() of
    ok  -> kflow_sup:start_link();
    Err -> Err
  end.

%% -----------------------------------------------------------------------------

stop(_State) ->
  ok.

%%=============================================================================
%% Internal functions:
%%=============================================================================

-spec load_pipe_config() -> ok | {error, term()}.
load_pipe_config() ->
  case {?cfg(config_module_dir), ?cfg(pipes)} of
    {undefined, _} ->
      %% Pipe configuration is baked into the OTP release, don't do
      %% anything:
      ok;
    {_, undefined} ->
      %% Pipe configuration is dynamic, don't do anything:
      ok;
    {Dir, {Module, _, _}} when is_list(Dir) ->
      Path = filename:join(Dir, atom_to_list(Module)),
      ?slog(notice, #{ what => "Loading pipe configuration"
                     , path => Path ++ ".erl"
                     }),
      Options = [binary, verbose, return],
      case compile:file(Path, Options) of
        {ok, Module, Binary, Warnings} ->
          ?slog(notice, #{ what => "Pipe configuration has been loaded"
                         , warnings => Warnings
                         }),
          code:delete(Module),
          code:purge(Module),
          code:load_binary(Module, Path, Binary),
          ok;
        {error, Errors, Warnings} ->
          ?slog(critical, #{ what     => "Failed to compile pipe config"
                           , errors   => Errors
                           , warnings => Warnings
                           }),
          {error, badconfig};
        error ->
          ?log(critical, "Failed to compile pipe config! Missing file?", []),
          {error, badconfig}
      end
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
