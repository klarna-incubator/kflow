%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%% @private
%%%===================================================================
-module(kflow_lib).

-include("kflow_int.hrl").

-export([ optional_callback/4
        , optional_callback/3
        , ensure_log/1
        , root_node_id/1
        , cfg/1
        , pretty_print_node_id/1
        , redirect_logs/1
        ]).

-spec optional_callback(module(), atom(), list()) -> ok.
optional_callback(Module, Function, Args) ->
  optional_callback(Module, Function, Args, ok).

-spec optional_callback(module(), atom(), list(), Default) -> Default.
optional_callback(Module, Function, Args, Default) ->
  Arity = length(Args),
  erlang:module_loaded(Module) orelse code:ensure_loaded(Module),
  case erlang:function_exported(Module, Function, Arity) of
    true ->
      apply(Module, Function, Args);
    false ->
      Default
  end.

-spec ensure_log(atom()) -> ok.
ensure_log(Id) ->
  Formatter       = ?cfg(pipe_log_formatter),
  DefaultLogLevel = ?cfg(pipe_log_level),
  LogLevels       = ?cfg(pipe_log_levels),
  LogDir          = ?cfg(log_dir),
  MyLogLevel      = maps:get(Id, LogLevels, DefaultLogLevel),
  File            = filename:join(LogDir, atom_to_list(Id)),
  Domain          = root_node_id(Id),
  Filter          = {fun logger_filters:domain/2, {log, sub, Domain}},
  logger:add_handler( Id
                    , logger_disk_log_h
                    , #{ level          => MyLogLevel
                       , config         => #{file => File}
                       , formatter      => Formatter
                       , filter_default => stop
                       , filters        => [{domain, Filter}]
                       }
                    ),
  ok.

-spec root_node_id(atom()) -> kflow:node_id().
root_node_id(Root) ->
  [kflow_pipe, Root].

-spec cfg(atom()) -> term().
cfg(Key) ->
  {ok, Val} = application:get_env(kflow, Key),
  Val.

-spec pretty_print_node_id(kflow:node_id()) -> file:filename_all().
pretty_print_node_id(NodeId) ->
  filename:join(['/' | NodeId]).

%% @private Small hack to make log messages of a process end up in our
%% dedicated log. `Pid' should be a `gen_server' or such.
-spec redirect_logs(pid() | atom()) -> ok.
redirect_logs(Pid) ->
  case logger:get_process_metadata() of
    #{domain := Domain} ->
      sys:replace_state(Pid,
                        fun(S) ->
                            logger:set_process_metadata(#{domain => Domain}),
                            S
                        end),
      ok;
    _ ->
      ok
  end.
