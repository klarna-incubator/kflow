%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc `kflow_gen' is a meta-behavior that all parts of the pipe
%%% (called <i>kfnodes</i> from this point on), such as aggregators
%%% and demultiplexers, are built upon. This module implements the
%%% following common scenarios:
%%%
%%% <ul><li>Startup stage where pids of the neighboring nodes have to
%%% be received (`initial' state)</li>
%%%
%%% <li>Reacting on upstream and downstream crashes</li>
%%%
%%% <li>Interaction with the master pipe process</li>
%%%
%%% <li>Backpressure</li>
%%%
%%% <li>Diagnostics, debugging, stuck pipe detection</li>
%%%
%%% <li>Keeping the state of callback module</li></ul>
%%%
%%% <i>Note:</i> this behavior is considered internal, and normally it
%%% shouldn't be used directly
%%%
%%% Some notes on terminology: modules that implement `kflow_gen'
%%% behavior from now on will be referred as "intermediate callback
%%% modules" or "intermediate CBMs". They typically define a behavior
%%% on their own; modules that implement such behavior will be called
%%% "user callback modules".
%%%
%%% == State transition diagram ==
%%% <img src="../images/kflow_gen_states.png"></img>
%%%
%%% == Message sequence diagrams ==
%%% === Handle message from the upstream ===
%%%
%%% Normal workflow as seen from the perspective of a kfnode:
%%%
%%% <ol><li>Processing of `?feed()' messages is postponed until the
%%% state machine is in `ready' state. As soon as it enters this
%%% state, it consumes one `?feed()' message from the queue. Business
%%% logic processing happens asynchronously: a temporary worker
%%% process is spawned, it executes the behavior callback
%%% `handle_message/3' and the FSM enters `working' state in the
%%% meanwhile</li>
%%%
%%% <li>As soon as the callback returns a success, the worker process
%%% replies to the parent process with `?done({ok, Messages,
%%% NewCbState})' message and terminates, where `Messages' is a
%%% (possibly empty) list of messages that should be sent downstream
%%% and `NewCbState' is the updated behavior state</li>
%%%
%%% <li>All messages returned by the callback should be sent
%%% downstream. The FSM may enter either `ready' or `blocked' state
%%% while the downstream is processing the messages, depending on the
%%% number of messages and backpressure settings</li>
%%%
%%% <li>As long as the number of messages queued up to the downstream
%%% is less than `max_queue_len' the kfnode can process messages from
%%% the upstream in parallel.</li>
%%%
%%% </ol>
%%%
%%% <img src="../images/message_from_upstream.png"></img>
%%%
%%% === Callback module failure ===
%%%
%%% <img src="../images/callback_failure.png"></img>
%%%
%%% === Handle upstream failure ===
%%%
%%% <img src="../images/upstream_failure.png"></img>
%%%
%%% === Handle downstream failure ===
%%%
%%% When the downstream dies, the kfnode receives
%%% `?downstream_failure' message. From this point it can't do all
%%% that much, so it just kills its worker process (if present) and
%%% terminates.
%%%
%%% == Intermediate CBM design guidelines ==
%%%
%%% Intermediate CBMs must follow certain rules.
%%%
%%% First of all, intermediate CBMs are fully responsible for offset
%%% tracking. They should not advance offset of output messages too
%%% far to avoid losing data when the pipe restarts.
%%%
%%% `hidden' flag of `#kflow_msg' record indicates that the message
%%% should never be passed to the user CBM: from user perspective such
%%% messages simply don't exist. However, the intermediate CBM must
%%% ensure that offsets of hidden messages are properly accounted for
%%% and committed. For example, when a pipe restarts, fully processed
%%% hidden messages should not be replayed. Intermediate CBMs can set
%%% this flag to `true'.
%%%
%%% `route' field of `#kflow_msg' should not be changed, unless
%%% intermediate CBM implements some kind of pipe splitting or joining
%%% operation. In which case only the head of `Route' list may be
%%% changed (added or removed), the tail must be preserved. Same goes
%%% for exposing route to the user CBM: probably it's a good idea to
%%% expose only the head of the route in order to improve
%%% composability.
%%%
%%% === Sub-streams ===
%%%
%%% One of design choices behind kflow was making sure that data flows
%%% strictly in one direction: from upstream to downstream. This makes
%%% reasoning about kflow pipes easier and eliminates many types of
%%% concurrency bugs. However it also makes stream splitting and
%%% joining somewhat tricky to implement. The biggest problem is
%%% offset tracking: kfnode must guarantee that it won't advance
%%% offset of messages that it sends downstream beyond safe value. And
%%% the upstream may buffer up some messages for unknown period of
%%% time.
%%%
%%% By default kflow framework solves this problem using the following
%%% trick. Each kfnode contains multiple states of user CBM, one state
%%% per `route' of upstream message. It's up to intermediate CBM to
%%% keep track of per-route states and multiplex messages between
%%% them.
%%%
%%% Remember that `route' field of `#kflow_msg{}' is a list. Stream
%%% splitting is done simply by adding a new element to the
%%% route. Conversely, stream joining is done by removing a head of
%%% the list. Apart from that, `route' field has no meaning. It is
%%% only used to look up user CBM state.
%%%
%%% Benefits of this solution:
%%%
%%% <ol>
%%% <li>Data flows in one direction</li>
%%%
%%% <li>Messages are always processed in the same order, so restarting
%%% the pipe is more likely to produce the same result; good for
%%% idempotency</li>
%%%
%%% <li>Easier to debug. No intermediate pipes are spawned, and not
%%% much message passing goes on in general.</li></ol>
%%%
%%% Downsides of this solution:
%%%
%%% <ol><li>All routes of the pipe are bound to the same
%%% topology. Solution: filter data that should be processed
%%% differently to separate Kafka topics and consume it from
%%% there.</li>
%%%
%%% <li>All routes of the pipe are processed sequentially. Solution:
%%% spread data across more Kafka partitions.</li></ol>
%%%
%%% @end
%%%===================================================================

-module(kflow_gen).

-behavior(gen_statem).

-include_lib("hut/include/hut.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("kflow.hrl").

%% API
-export([ feed/4
        , flush/1
        , get_status/1
        , start_link/1
        , post_init/2
        , notify_upstream_failure/1
        ]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3]).
-export([handle_event/4]).

-export_type([callback_return/1, ret_type/0]).

%%%===================================================================
%%% Types and macros:
%%%===================================================================

%% pipe-node protocol:
-define(post_init(Neighbors), {post_init, Neighbors}).

%% worker-node protocol:
-define(done(Result),         {done, Result}).

%% node-node protocol:
-define(flush,                flush).
-define(feed(Ref, Msg),       {upstream, Ref, Msg}).
-define(ack(Ref),             {ack, Ref}).
-define(upstream_failure,     upstream_failure).
-define(downstream_failure,   downstream_failure).

%% State-specific data for `working' state:
-record(working,
        { worker_pid :: pid()
        , ack_ref    :: reference() | undefined
        }).

-type state_specific_data() ::
        #working{}
      | undefined
      .

-record(data,
        { id                       :: kflow:node_id()
          %% Kflow framework module implementing the behavior:
        , cb_module                :: module()
        , config                   :: term()
          %% Callback module state:
        , cb_state                 :: term()

        , upstream_pid             :: pid()
        , downstream               :: pid() | undefined

        , offset_in                :: integer() | undefined
        , offset_out               :: integer() | undefined

          %% Backpressure is engaged when queue is longer than this:
        , downstream_queue_max = 1 :: non_neg_integer()
        , downstream_queue = []    :: [kflow:message() | ?flush]
        , downstream_ack_ref       :: reference() | undefined

        , state_specific           :: state_specific_data()
        }).

-type data() :: #data{}.

-type state() :: initial            %% Waiting for the neighboring pids
               | ready              %% Waiting for a message from the upstream
               | {working, state()} %% Executing a callback (keep previous state)
               | blocked            %% Blocked by backpressure from the downstream
               | exiting            %% The upstream has crashed, flushing messages before dying
               .

-type ret_type() :: ok
                  | exit %% TODO: Currently only `kflow_multistate' understands this.
                  .

-type callback_return(State) :: {ret_type(), [kflow:message()], State}.

%%%===================================================================
%%% Callback definitions:
%%%===================================================================

%% Initialize the callback module:
-callback init(kflow:node_id(), _Config) -> {ok, _State}.

%% Handle message from the upstream:
-callback handle_message(kflow:message(), State, _Config) ->
  callback_return(State).

%% Handle flush:
-callback handle_flush(State, _Config) ->
  callback_return(State).

%% Handle graceful shutdown:
-callback terminate(_State, _Config) -> _.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Send a message to the kfnode and block the caller until the
%% message is processed by <i>this</i> node (but the subsequent
%% processing is done asynchronously)
%%
%% Note: second argument of the function is a monitor reference of the
%% corresponding `kflow_pipe' process, rather than `kflow_gen'
%% process! This is done to let internal fault handling logic run
%% before crashing the caller.
%%
%% @end
%% TODO: Control flow of feeding the pipe is way too convoluted, this
%% should be refactored.
-spec feed(pid(), reference(), kflow:message(), timeout()) ->
              ok | {error, _}.
feed(Pid, MRef, Msg, Timeout) ->
  gen_statem:cast(Pid, ?feed(MRef, Msg)),
  receive
    ?ack(MRef) ->
      demonitor(MRef, [flush]),
      ok;
    {'DOWN', MRef, process, _, _} ->
      {error, pipe_failure}
  after Timeout ->
      demonitor(MRef, [flush]),
      {error, timeout}
  end.

%% @doc Command the node and its downstream to immediately flush all
%% the buffered data (async call)
-spec flush(pid()) -> ok.
flush(Server) ->
  gen_statem:cast(Server, ?flush).

%% @doc Tell the node pids of its neighbors
-spec post_init(pid(), {_Upstream :: pid(), _Downstream :: pid()}) -> ok.
post_init(Pid, Neighbors) ->
  gen_statem:call(Pid, ?post_init(Neighbors)).

%% @doc Get various debug information about the node
-spec get_status(pid()) -> term(). %% TODO: KC-1192
get_status(Pid) ->
  gen_statem:call(Pid, get_status).

%% @doc Start a kfnode process
-spec start_link(#init_data{}) -> {ok, pid()}.
start_link(InitData) ->
  gen_statem:start(?MODULE, InitData, []).

%% @doc Nicely ask node to stop
-spec notify_upstream_failure(pid() | undefined) -> ok.
notify_upstream_failure(Pid) when is_pid(Pid) ->
  gen_statem:cast(Pid, ?upstream_failure);
notify_upstream_failure(undefined) ->
  ok.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

%% @private
-spec init(InitData :: #init_data{}) ->
              gen_statem:init_result(state()).
init(InitData = #init_data{id = Id}) ->
  ?set_process_metadata(#{domain => Id}),
  ?slog(info, #{ what         => "Starting kflow_gen process"
               , initial_data => InitData
               , pid          => self()
               }),
  {ok, initial, InitData}.

%% @private FSM
%% Initial:
handle_event({call, From}, ?post_init(Neighbors), initial, InitialData) ->
  handle_post_init(From, Neighbors, InitialData);
%% Working:
handle_event(cast, ?done(Result), {working, OldState}, Data) ->
  handle_done(Result, OldState, Data);
%% Ready:
handle_event(cast, ?feed(Ref, Msg), ready, Data) ->
  async_callback(Ref, Msg, ready, Data);
handle_event(cast, ?flush, ready, Data) ->
  async_callback(undefined, ?flush, ready, Data);
handle_event(cast, ?upstream_failure, ready, Data) ->
  ?slog(debug, #{ what => "Upstream failure"
                , id   => Data#data.id
                }),
  async_callback(undefined, ?flush, exiting, Data);
%% Exiting:
handle_event(enter, OldState, exiting, Data) ->
  case Data#data.downstream_ack_ref of
    undefined ->
      %% We don't need to feed anything downstream, exit immediately
      stop;
    _ ->
      %% Wait to flush the buffered data
      common_state_enter(OldState, exiting, Data)
  end;
handle_event(timeout, exit_timeout, exiting, Data) ->
  ?slog(error, #{ what => "kflow_gen shutdown timeout"
                , data => Data
                , self => self()
                }),
  %% Downstream is too slow, so just drop buffered data:
  stop;
%% Common:
handle_event(info, ?ack(Ref), State, Data) ->
  #data{ downstream_ack_ref = Ref %% <- Assert
       } = Data,
  handle_ack(Ref, State, Data);
handle_event(cast, ?feed(_Ref, _Msg), _State, _Data) ->
  {keep_state_and_data, [postpone]};
handle_event(cast, ?flush, _State, _Data) ->
  {keep_state_and_data, [postpone]};
handle_event(cast, ?upstream_failure, _State, _Data) ->
  {keep_state_and_data, [postpone]};
handle_event(cast, ?downstream_failure, _State, _Data) ->
  stop;
handle_event(enter, OldState, State, Data) ->
  common_state_enter(OldState, State, Data);
handle_event(Event, Msg, State, _Data) ->
  ?slog(warning, #{ what  => "kflow_gen unknown event"
                  , event => Event
                  , data  => Msg
                  , state => State
                  , self  => self()
                  }),
  keep_state_and_data.

%% @private
terminate(_Reason, _State, #data{ cb_state     = CbState
                                , config       = Config
                                , cb_module    = CbModule
                                , upstream_pid = Upstream
                                , downstream   = Downstream
                                }) ->
  notify_upstream_failure(Downstream),
  %% Just for a good measure: this is probably redundant:
  notify_downstream_failure(Upstream),
  CbModule:terminate(CbState, Config),
  void;
terminate(_Reason, _State, _Data) ->
  void.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec common_state_enter(state(), state(), data()) ->
                            gen_state:state_enter_result(state()).
common_state_enter(initial, initial, Data) ->
  {next_state, initial, Data};
common_state_enter(OldState, State, Data) ->
  ?tp(kflow_gen_state_transition,
      #{ state_from => OldState
       , state_to   => State
       , pid        => self()
       , id         => Data#data.id
       }),
  {next_state, State, Data}.

-spec handle_post_init(gen_statem:from(), {pid(), pid()}, #init_data{}) ->
                          gen_statem:event_handler_result(state()).
handle_post_init(From, {Upstream, Downstream}, InitialData) ->
  link(Upstream),
  #init_data{ id            = Id
            , cb_module     = CbModule
            , config        = Config
            , max_queue_len = MaxLen
            } = InitialData,
  %% Here we crash if init fails:
  {ok, CbState} = CbModule:init(Id, Config),
  Data = #data{ id                   = Id
              , cb_module            = CbModule
              , config               = Config
              , cb_state             = CbState
              , upstream_pid         = Upstream
              , downstream           = Downstream
              , downstream_queue_max = MaxLen
              },
  {next_state, ready, Data, [{reply, From, ok}]}.

%% @private Handle the return message of middleman process
-spec handle_done(term(), state(), data()) ->
                     gen_statem:event_handler_result(state()).
handle_done(Result, OldState, Data0) ->
  #data{ upstream_pid   = Upstream
       , state_specific = #working{ack_ref = Ref}
       , id             = Id
       } = Data0,
  Ref =/= undefined andalso ack(Upstream, Ref),
  case Result of
    {ok, NewMessages, CbState} ->
      Data = Data0#data{ cb_state       = CbState
                       , state_specific = undefined
                       },
      queue_up_downstream_messages( OldState
                                  , NewMessages
                                  , Data
                                  );
    WrongResult ->
      case WrongResult of
        {EC, Error, Stack} when EC =:= error;
                                EC =:= exit;
                                EC =:= throw ->
          ?slog(critical, #{ what       => "Kflow node callback crash"
                           , error      => {EC, Error}
                           , stacktrace => Stack
                           , node_id    => Id
                           });
        _ ->
          ?slog(critical, #{ what       => "Kflow node invalid callback return"
                           , return     => WrongResult
                           , node_id    => Id
                           })
      end,
      notify_downstream_failure(Upstream),
      {next_state, exiting, Data0}
  end.

-spec handle_ack(reference(), state(), data()) ->
                    gen_statem:event_handler_result(state()).
handle_ack(Ref, exiting, #data{downstream_queue = []}) ->
  erlang:demonitor(Ref, [flush]),
  stop;
handle_ack(Ref, OldState, Data0) ->
  erlang:demonitor(Ref, [flush]),
  Data1 = Data0#data{downstream_ack_ref = undefined},
  feed_downstream(OldState, Data1).

-spec queue_up_downstream_messages(state(), [kflow:message()], data()) ->
                         gen_statem:event_handler_result(state()).
queue_up_downstream_messages(OldState, NewMessages, Data0) ->
  #data{ downstream_queue     = OldMessages
       , downstream_ack_ref   = AckRef
       } = Data0,
  Queue = OldMessages ++ NewMessages,
  Data = Data0#data{downstream_queue = Queue},
  case AckRef of
    undefined ->
      %% We don't expect any acks from the downstream, let us feed it
      %% some data:
      feed_downstream(OldState, Data);
    _ when is_reference(AckRef) ->
      %% We already have a pending transfer, just add messages to the
      %% queue
      {next_state, next_state(OldState, Data), Data}
  end.

-spec feed_downstream(state(), data()) ->
                         gen_statem:event_handler_result(state()).
feed_downstream(OldState, Data0 = #data{downstream = undefined}) ->
  %% No downstream, all messages go straight to /dev/null:
  Data = Data0#data{downstream_queue = []},
  {next_state, next_state(OldState, Data), Data};
feed_downstream(OldState, Data = #data{downstream_queue = []}) ->
  %% Nothing to feed.
  {next_state, next_state(OldState, Data), Data};
feed_downstream(OldState, Data0) ->
  #data{ downstream         = Downstream
       , downstream_ack_ref = OldAckRef
       , downstream_queue   = [Msg|Rest]
       }                    = Data0,
  undefined = OldAckRef, %% assert
  {async, AckRef} = do_feed(Downstream, Msg),
  Data = Data0#data{ downstream_ack_ref = AckRef
                   , downstream_queue   = Rest
                   },
  {next_state, next_state(OldState, Data), Data}.

%% @private
-spec do_feed(pid(), kflow:message()) -> {async, reference()} | ok.
do_feed(Pid, Msg) when is_pid(Pid) ->
  Ref = erlang:monitor(process, Pid),
  gen_statem:cast(Pid, ?feed(Ref, Msg)),
  {async, Ref}.

%% @private
-spec notify_downstream_failure(pid()) -> ok.
notify_downstream_failure(Pid) ->
  gen_statem:cast(Pid, ?downstream_failure).

%% @private
-spec ack(pid(), reference()) -> ok.
ack(Pid, Ref) ->
  Pid ! ?ack(Ref),
  ok.

%% @private Execute the callback in a temporary middleman process:
-spec async_callback( reference() | undefined
                    , kflow:message() | ?flush
                    , state()
                    , data()
                    ) -> gen_statem:event_handler_result(state()).
async_callback(Ref, Input, OldState, Data0) ->
  Self = self(),
  #data{ cb_module = CbModule
       , config    = Config
       , cb_state  = CbState
       , id        = Id
       , offset_in = OffsetIn0
       } = Data0,
  Pid = spawn_link(fun() ->
                       run_callback(Self, Id, CbModule, CbState, Config, Input)
                   end),
  WorkingData = #working{ worker_pid = Pid
                        , ack_ref = Ref
                        },
  case Input of
    #kflow_msg{offset = OffsetIn} -> ok;
    ?flush                        -> OffsetIn = OffsetIn0
  end,
  Data = Data0#data{ state_specific = WorkingData
                   , offset_in      = OffsetIn
                   },
  {next_state, {working, OldState}, Data}.

%% @private This function runs in a worker process.
-spec run_callback( pid()
                  , kflow:node_id()
                  , module()
                  , _CbState
                  , _CbConfig
                  , kflow:message() | ?flush
                  ) -> ok.
run_callback(Parent, Id, CbModule, CbState, Config, Input) ->
  ?set_process_metadata(#{domain => Id}),
  Result = try
             case Input of
               #kflow_msg{} ->
                 CbModule:handle_message(Input, CbState, Config);
               ?flush ->
                 propagate_flush(CbModule:handle_flush(CbState, Config))
             end
           catch
             EC:Err:Stacktrace ->
               {EC, Err, Stacktrace}
           end,
  gen_statem:cast(Parent, ?done(Result)).

%% @private Ask downstream buffer to flush after processing the last
%% batch of messages.
-spec propagate_flush(callback_return(S)) -> callback_return(S).
propagate_flush({ok, Messages, State}) ->
  {ok, Messages ++ [?flush], State}.

%% @private Calculate next state
-spec next_state(state(), data()) -> state().
next_state(OldState, Data) ->
  #data{ downstream_queue_max = MaxLen
       , downstream_queue     = Queue
       } = Data,
  case OldState of
    exiting ->
      exiting;
    {working, PrevState} ->
      {working, PrevState};
    _ when length(Queue) < MaxLen ->
      ready;
    _ ->
      blocked
  end.
