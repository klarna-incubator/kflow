%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Klarna Bank AB
%%% @doc This module implements interface towards pipe.
%%%
%%% <i>Note:</i>It does not require starting `kflow' root supervisor,
%%% so it can be reused in other applications.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(kflow_pipe).

-behavior(gen_statem).

-include("kflow_int.hrl").
-include_lib("hut/include/hut.hrl").

%% API
-export([start_link/1, stop/1]).

%% Internal imports
-export([desugar_node_spec/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3]).
-export([handle_event/4]).

%%%===================================================================
%%% Types
%%%===================================================================

-type entrypoint() :: fun((kflow:message() | flush) -> ok).

-type pipe_config() ::
        #{ id               := kflow:node_id()
         , definition       := kflow:pipe()
         , feed_timeout     => timeout()
         , shutdown_timeout => timeout()
         , flush_interval   => non_neg_integer() | undefined
         }.

-type state() :: normal | exiting.

-record(data,
        { id                    :: kflow:node_id()
        , children              :: #{integer() => pid()}
        , num_children          :: non_neg_integer()
        , feed_timeout          :: timeout()
        , shutdown_timeout      :: timeout()
        , shutdown_ref = []     :: [gen_statem:from()]
        , flush_interval        :: non_neg_integer() | undefined
        , flush_timer           :: reference() | undefined
        , expected_stop = false :: boolean()
        }).

-type data() :: #data{}.

-export_type([pipe_config/0, entrypoint/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start an instance of a pipe.
%% Specification of a pipe is a map with the following keys:
%%
%% <ul><li>`id': globally unique identifier of the pipe, that
%% should be a list of atoms. Mandatory.</li>
%%
%% <li>`definition': list of stream processing nodes that make up the
%% pipe. Mandatory.</li>
%%
%% <li>`feed_timeout': crash if feeding message into the pipes takes
%% longer than so many milliseconds. Optional, `infinity' by
%% default.</li>
%%
%% <li>`shutdown_timeout': brutally kill the pipe if graceful shutdown
%% takes longer than so many milliseconds. Optional, `infinity' by
%% default.</li>
%%
%% </ul>
%%
%% This function returns pid of the pipe and `Feed' closure that is
%% used to feed messages into the pipe. Messages should be
%% `#kflow_msg' records. Example:
%%
%% ```
%% PipeSpec = [{filter, fun(_Offset, Msg) -> is_integer(Msg) end},
%%             {map, fun(_Offset, Msg) -> Msg + 1 end},
%%             {map, fun(_Offset, Msg) -> erlang:display(Msg), Msg end}
%%            ],
%% {ok, Pid, Feed} = kflow_pipe:start(#{id := [test], definition := PipeSpec}),
%% Feed(#kflow_msg{offset = 1, payload = foo}),
%% Feed(#kflow_msg{offset = 2, payload = 1}),
%% Feed(#kflow_msg{offset = 3, payload = 2}),
%% kflow_pipe:stop(Pid).
%% '''
-spec start_link(pipe_config()) -> {ok, pid(), entrypoint()}
                                 | {error, empty_pipe}.
start_link(#{definition := []}) ->
  {error, empty_pipe};
start_link(PipeConfig) ->
  {ok, Pid} = gen_statem:start_link(?MODULE, {PipeConfig, self()}, []),
  {ok, Entrypoint} = gen_statem:call(Pid, get_entrypoint),
  {ok, Pid, Entrypoint}.

%% @doc Gracefully stops the pipe, waiting until all in-flight
%% messages are fully processed and stream processing nodes are
%% terminated.
-spec stop(pid()) -> ok.
stop(Pid) ->
  gen_statem:call(Pid, stop, infinity).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> handle_event_function.

%% @private
-spec init({pipe_config(), pid()}) ->
              gen_statem:init_result(term()).
init({PipeConfig, Source}) ->
  process_flag(trap_exit, true),
  #{id := PipeId, definition := Specs} = PipeConfig,
  ?set_process_metadata(#{domain => PipeId}),
  FeedTimeout     = maps:get(feed_timeout, PipeConfig, 30000),
  ShutdownTimeout = maps:get(shutdown_timeout, PipeConfig, 30000),
  FlushInterval   = maps:get(flush_interval, PipeConfig, undefined),
  NumChildren     = length(Specs),
  Ids             = lists:seq(0, NumChildren - 1),
  Children = maps:from_list(lists:map( fun({I, Spec}) ->
                                           start_child(PipeId, I, Spec)
                                       end
                                     , lists:zip(Ids, Specs)
                                     )),
  ExtendedChildren = Children #{ -1          => Source %% TODO: make it self()
                               , NumChildren => undefined
                               },
  lists:foreach( fun(I) ->
                     Up   = I - 1,
                     Down = I + 1,
                     #{I := Pid, Up := PidU, Down := PidD} = ExtendedChildren,
                     kflow_gen:post_init(Pid, {PidU, PidD})
                 end
               , Ids
               ),
  Data = #data{ id               = PipeId
              , children         = Children
              , num_children     = NumChildren
              , feed_timeout     = FeedTimeout
              , shutdown_timeout = ShutdownTimeout
              , flush_interval   = FlushInterval
              , flush_timer      = start_flush_timer(FlushInterval)
              },
  {ok, normal, Data}.

%% @private
handle_event({call, From}, get_entrypoint, _State, Data) ->
  handle_get_entrypoint(From, Data);
handle_event({call, From}, stop, normal, Data) ->
  handle_stop(From, Data#data{expected_stop = true});
handle_event({call, From}, stop, exiting, Data0) ->
  Data = Data0#data{shutdown_ref = [From|Data0#data.shutdown_ref]},
  {keep_state, Data};
handle_event(info, flush, normal, Data) ->
  handle_flush(Data);
handle_event(info, {'DOWN', _Ref, process, Pid, Reason}, State, Data) ->
  handle_down(Pid, Reason, State, Data);
handle_event({call, From}, Msg, State, _Data) ->
  ?slog(warning, #{ what  => "kflow_pipe unknown call"
                  , from  => From
                  , data  => Msg
                  , state => State
                  , self  => self()
                  }),
  Ret = {error, unknown_call},
  {keep_state_and_data, [{reply, From, Ret}]};
handle_event(Event, Msg, State, _Data) ->
  ?slog(warning, #{ what  => "kflow_pipe unknown event"
                  , event => Event
                  , data  => Msg
                  , state => State
                  , self  => self()
                  }),
  keep_state_and_data.

%% @private
terminate(Reason, _State, Data) ->
  #data{ id           = Id
       , children     = Children
       } = Data,
  ?slog(info, #{ what   => "kflow_pipe shutting down"
               , self   => self()
               , id     => Id
               , reason => Reason
               }),
  maps:map( fun(_, Pid) when is_pid(Pid) ->
                exit(Pid, kill);
               (_, _) ->
                ok
            end
          , Children),
  void.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_get_entrypoint(gen_statem:from(), data()) ->
                               gen_statem:event_handler_result(state()).
handle_get_entrypoint(From, Data) ->
  #data{ children     = Children
       , feed_timeout = Timeout
       } = Data,
  Self = self(),
  #{0 := Pid} = Children,
  Ret = fun(Msg) ->
            MRef = monitor(process, Self),
            kflow_gen:feed(Pid, MRef, Msg, Timeout)
        end,
  {keep_state_and_data, [{reply, From, {ok, Ret}}]}.

-spec handle_down(pid(), term(), state(), data()) ->
                     gen_statem:event_handler_result(state()).
handle_down(Pid, Reason, exiting, Data) ->
  #data{ children      = Children
       , num_children  = N
       , id            = Id
       , shutdown_ref  = ShutdownRef
       , expected_stop = ExpectedStop
       } = Data,
  IsLast = maps:get(N - 1, Children) =:= Pid,
  IsChild = my_childp(Pid, Data),
  if IsLast ->
      %% Last child terminated, we can stop:
      ?slog(info, #{ what   => "kflow_pipe last node terminated; pipe process exiting"
                   , self   => self()
                   , id     => Id
                   , pid    => Pid
                   , reason => Reason
                   }),
      Replies = [{reply, I, ok} || I <- ShutdownRef],
      gen_statem:reply(Replies),
      Reason = case ExpectedStop of
                 true  -> normal;
                 false -> kflow_pipe_crash
               end,
      {stop, Reason};
     IsChild ->
      ?slog(info, #{ what   => "kflow_pipe node terminated; keep waiting for the last one"
                   , self   => self()
                   , id     => Id
                   , pid    => Pid
                   }),
      keep_state_and_data;
     true ->
      ?slog(warning, #{ what   => "kflow_pipe received unexpected DOWN message"
                      , self   => self()
                      , pid    => Pid
                      , reason => Reason
                      }),
      keep_state_and_data
  end;
handle_down(Pid, Reason, _State, Data) ->
  case my_childp(Pid, Data) of
    true ->
      {next_state, exiting, Data, [postpone]};
    false ->
      ?slog(warning, #{ what   => "kflow_pipe received unexpected DOWN message"
                      , self   => self()
                      , pid    => Pid
                      , reason => Reason
                      }),
      keep_state_and_data
  end.

-spec handle_flush(data()) -> gen_statem:event_handler_result(state()).
handle_flush(Data0) ->
  #data{ children       = Children
       , flush_interval = FlushInterval
       , id             = MyId
       } = Data0,
  #{0 := Pid} = Children,
  ?slog(info, #{ what => "Scheduled pipe flush"
               , id   => MyId
               }),
  kflow_gen:flush(Pid),
  Data = Data0#data{ flush_timer = start_flush_timer(FlushInterval)
                   },
  {keep_state, Data}.

%% @private
-spec my_childp(pid(), data()) -> boolean().
my_childp(Pid, #data{children = Children}) ->
  lists:member(Pid, maps:values(Children)).

%% @private
-spec start_child(kflow:node_id(), integer(), kflow:node_spec()) ->
                     {integer(), pid()}.
start_child(PipeId, I, NodeSpec) ->
  {Behavior, NodeConfig, Config} = desugar_node_spec(NodeSpec),
  Id = list_to_atom(integer_to_list(I)),
  CbTimeout   = maps:get(hard_timeout, NodeConfig, infinity),
  MaxQueueLen = maps:get(max_queue_len, NodeConfig, 1),
  InitData    = #init_data{ id            = PipeId ++ [Id]
                          , cb_module     = Behavior
                          , config        = Config
                          , cb_timeout    = CbTimeout
                          , max_queue_len = MaxQueueLen
                          },
  {ok, Pid} = kflow_gen:start_link(InitData),
  monitor(process, Pid),
  {I, Pid}.

-spec handle_stop(gen_statem:from(), data()) -> gen_statem:callback_return(state()).
handle_stop(From, Data0 = #data{shutdown_timeout = Timeout, children = Children}) ->
  case lists:sort(maps:to_list(Children)) of
    [{0, Pid} | _] ->
      kflow_gen:notify_upstream_failure(Pid),
      Actions = case Timeout of
                  infinity -> [];
                  _        -> [{timeout, Timeout, exit_timeout}]
                end,
      Data = Data0#data{shutdown_ref = [From]},
      {next_state, exiting, Data, Actions};
    [] ->
      {stop, [{reply, From, ok}]}
  end.

%% @private
-spec desugar_node_spec(kflow:node_spec()) -> {module(), kflow:node_config(), term()}.
desugar_node_spec(join) ->
  {kflow_join, #{}, {void, void}};
desugar_node_spec({route_dependent, Fun}) ->
  true = is_function(Fun, 1), %% Assert
  {kflow_route_dependent, #{}, Fun};
desugar_node_spec({Behavior, Fun}) when is_function(Fun),
                                        Behavior =:= map orelse
                                        Behavior =:= filter orelse
                                        Behavior =:= demux orelse
                                        Behavior =:= unfold orelse
                                        Behavior =:= mfd ->
  Module = translate_behavior(Behavior),
  {Module, #{}, {Module, Fun}};
desugar_node_spec({Behavior, CbModule, CbConfig}) when
    is_atom(Behavior), is_atom(CbModule) ->
  {translate_behavior(Behavior), #{}, {CbModule, CbConfig}};
desugar_node_spec({Behavior, NodeConfig = #{}, CbModule, CbConfig}) when
    is_atom(Behavior), is_atom(CbModule) ->
  {translate_behavior(Behavior), NodeConfig, {CbModule, CbConfig}}.

%% @private Expand short behavior name into full module name
-spec translate_behavior(kflow:behavior()) -> module().
translate_behavior(map) ->
  kflow_gen_map;
translate_behavior(aggregate) ->
  kflow_gen_aggregate;
translate_behavior(filter) ->
  kflow_gen_filter;
translate_behavior(mfd) ->
  kflow_gen_mfd;
translate_behavior(demux) ->
  kflow_gen_demux;
translate_behavior(unfold) ->
  kflow_gen_unfold;
translate_behavior(assemble_chunks) ->
  kflow_gen_assemble_chunks;
translate_behavior(A) ->
  A.

-spec start_flush_timer(non_neg_integer() | undefined) -> reference() | undefined.
start_flush_timer(undefined) ->
  undefined;
start_flush_timer(Interval) ->
  erlang:send_after(Interval, self(), flush).
