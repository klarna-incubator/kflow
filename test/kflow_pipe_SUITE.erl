-module(kflow_pipe_SUITE).

-compile(export_all).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include_lib("kflow/src/testbed/payload_gen.hrl").
-include("kflow.hrl").

-define(dt, 10).

-define(default_route, <<>>).

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_suite(Config) ->
  snabbkaffe:fix_ct_logging(),
  [{proper, [ {timeout, 10000}
            , {numtests, 100}
            ]}
   | Config].

end_per_suite(_Config) ->
  ok.

suite() -> [{timetrap, {seconds, 300}}].

%%====================================================================
%% Testcases
%%====================================================================

%% Check that messages get relayed unchanged through the chain of
%% simple maps (`kflow_test_map' module doesn't do anything to the
%% message, just emits a trace that it saw the message)
gen_map_prop() ->
  MaxDelay = 5,
  MaxNumMaps = 10,
  ?forall_trace(
     %% Input data:
     {Messages, NumMaps, FeedTimeout}, {messages(), range(1, MaxNumMaps), feed_timeout(MaxDelay)},
     %% Snabbkaffe settings:
     #{bucket => length(Messages)},
     %% Run stage:
     begin
       PipeDefn = [gen_map_spec(MaxDelay, I) || I <- lists:seq(0, NumMaps - 1)],
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id           => [test_pipe]
                                                 , definition   => PipeDefn
                                                 , feed_timeout => FeedTimeout
                                                 }),
       feed_pipe(Feed, Messages),
       kflow_pipe:stop(Pipe),
       {PipeDefn, Messages}
     end,
     %% Check stage:
     fun({PipeDefn, Messages}, Trace) ->
         kflow_trace_specs:message_conservation(PipeDefn, Messages, Trace),
         kflow_trace_specs:message_order(PipeDefn, Trace)
     end).

t_gen_map(Config) when is_list(Config) ->
  ?run_prop(Config, gen_map_prop()).

all_behaviors_prop() ->
  ?forall_trace(
     %% Input data:
     {Messages, Specs}, {messages(), non_empty(list(kfnode(5)))},
     %% Snabbkaffe settings:
     #{bucket => length(Messages), timeout => ?dt},
     %% Run stage:
     begin
       PipeDefn = add_node_ids(Specs),
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeDefn
                                                 }),
       feed_pipe(Feed, Messages),
       ok = kflow_pipe:stop(Pipe),
       {PipeDefn, Messages}
     end,
     %% Check stage:
     fun({PipeDefn, Messages}, Trace) ->
         kflow_trace_specs:message_conservation(PipeDefn, Messages, Trace),
         kflow_trace_specs:message_order(PipeDefn, Trace),
         kflow_trace_specs:init_terminate(Trace),
         true
     end).

t_all_behaviors(Config) when is_list(Config) ->
  ?run_prop(Config, all_behaviors_prop()).

gen_aggregate_prop() ->
  ?forall_trace(
     %% Input data:
     {Messages, Specs}, {messages(), non_empty(list(map_or_aggregate(5)))},
     %% Snabbkaffe settings:
     #{bucket => length(Messages), timeout => ?dt},
     %% Run stage:
     begin
       PipeDefn = add_node_ids(Specs),
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeDefn
                                                 }),
       feed_pipe(Feed, Messages),
       ok = kflow_pipe:stop(Pipe),
       {PipeDefn, Messages}
     end,
     %% Check stage:
     fun({PipeDefn, Messages}, Trace) ->
         kflow_trace_specs:message_conservation(PipeDefn, Messages, Trace),
         kflow_trace_specs:message_order(PipeDefn, Trace),
         kflow_trace_specs:init_terminate(Trace),
         [begin
            kflow_trace_specs:check_aggregation(Config, Trace),
            kflow_trace_specs:state_continuity(Config, Trace)
          end || {aggregate, kflow_test_aggregate, Config} <- PipeDefn],
         true
     end).

t_gen_aggregate(Config) when is_list(Config) ->
  ?run_prop(Config, gen_aggregate_prop()).

t_flush_on_crash(Config) when is_list(Config) ->
  NMessages = 100,
  %% 1st node should process half of the payload before crashing:
  CrashOffset = NMessages div 2,
  Messages = lists:seq(1, NMessages),
  PipeSpec = [ {map, kflow_test_map, #{id => '1'}}
             , {aggregate, kflow_test_aggregate,
                #{ id => 2
                 , size => NMessages
                 }}
             ],
  ?check_trace(
     %% Run stage:
     begin
       ?inject_crash( #{ ?snk_kind := kflow_test_map_seen_message
                       , offset    := CrashOffset
                       }
                    , snabbkaffe_nemesis:always_crash()
                    ),
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeSpec
                                                 }),
       %% We expect this process to crash:
       unlink(Pipe),
       MRef = monitor(process, Pipe),
       catch feed_pipe(Feed, Messages),
       receive
         {'DOWN', MRef, process, Pipe, _} -> ok
       after 10000 ->
           error(would_not_die)
       end
     end,
     %% Check stage:
     fun(_Result, Trace) ->
         %% 1. Check that node number 1 stopped processing after first
         %% crash:
         LastSuccOffset = CrashOffset - 1,
         ?assertEqual( lists:seq(1, LastSuccOffset)
                     , ?projection( offset
                                  , ?of_kind(kflow_test_map_seen_message, Trace)
                                  )
                     ),
         %% 2. Check that nodes 2 flushed buffered messages:
         ?assertMatch( [#{offset := LastSuccOffset}]
                     , ?of_kind(kflow_test_aggregate_out, Trace)
                     )
     end).

%% Basic checks regarding stream splitting and joining:
substreams_prop() ->
  AggregateConfig = #{id => '1', size => 1000000},
  PipeDefn = [ {demux, fun(_Offset, Msg) -> maps:get(route, Msg) end}
             , {aggregate, kflow_test_aggregate, AggregateConfig}
             , join
             , {map, kflow_test_map, #{id => '2'}}
             ],
  ?forall_trace(
     %% Input data:
     Streams, payload_gen:interleaved_list_gen(message()),
     %% Run stage:
     begin
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeDefn
                                                 }),
       feed_pipe(Feed, payload_gen:interleave_streams(Streams)),
       ok = kflow_pipe:stop(Pipe)
     end,
     %% Check stage:
     fun(_Stream, Trace) ->
         {Routes, _} = lists:unzip(Streams),
         %% Check that messages from all routes end up in the joined
         %% stream, exactly once:
         OutputMessageRoutes = [Route || #{ ?snk_kind := kflow_test_map_seen_message
                                          , id        := '2'
                                          , payload   := [#{route := Route}|_]
                                          } <- Trace],
         ?assertEqual(lists:sort(Routes), lists:sort(OutputMessageRoutes)),
         Fun = fun(Route) ->
                   %% Find messages that belong only to the current route:
                   FilteredTrace = [E || E = #{payload := #{route := Route1}} <- Trace
                                       , Route1 =:= Route],
                   InputMessages = ?of_kind(test_feed_message, FilteredTrace),
                   %% Check that state of `gen_aggregate' is updated in a
                   %% consistent manner for each route:
                   kflow_trace_specs:state_continuity(AggregateConfig, FilteredTrace),
                   %% Check that every message in the route was seen by the aggregate:
                   In = [P || #{ ?snk_kind := kflow_test_aggregate_in
                               , payload   := P
                               } <- FilteredTrace],
                   snabbkaffe:push_stat(n_messages, length(In)),
                   ?assertEqual(?projection(payload, InputMessages), In)
               end,
         lists:foreach(Fun, Routes),
         true
     end).

t_substreams(Config) when is_list(Config) ->
  ?run_prop(Config, substreams_prop()).

t_route_dependent(Config) when is_list(Config) ->
  PipeDefn =
    [ {demux, fun(_Offset, #{route := Route}) ->
                  Route
              end}
    , {route_dependent,
         fun(RouteHd) ->
             {map, kflow_test_map, #{id => RouteHd}}
         end}
    ],
  ?check_trace(
     %% Run stage:
     begin
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeDefn
                                                 }),
       Msgs = [{R, I} || R <- [foo, bar, baz]
                       , I <- lists:seq(1, 10)
                       ],
       feed_pipe(Feed, Msgs),
       ok = kflow_pipe:stop(Pipe),
       Msgs
     end,
     %% Check stage:
     fun(ExpectedMsgs0, Trace) ->
         %% Here we verify that `id' of the pipe that processed
         %% messages matches with the intended value.
         %% Prepare reference trace:
         ExpectedMsgs = [{R, #{ route => R
                              , value => I
                              }}
                         || {R, I} <- ExpectedMsgs0],
         %% Extract relevant information from the real trace:
         Msgs = ?projection( [id, payload]
                           , ?of_kind(kflow_test_map_seen_message, Trace)
                           ),
         ?assertEqual(ExpectedMsgs, Msgs)
     end).

%% Check how long does it take to pass 1000 messages through a series
%% of pipes doing nothing:
throughput_bench() ->
  NMessages = 1000,
  ?forall_trace(
     %% Input data:
     NumNodes, range(0, 49),
     %% Snabbkaffe settings:
     #{bucket => NumNodes},
     %% Run stage:
     begin
       Messages = lists:seq(1, NMessages),
       Fun = fun(_, #{value := A}) when A =:= NMessages ->
                 ?tp(complete, #{});
                (_, _) ->
                 ok
             end,
       PipeDefn = [{map, fun(_, A) -> A end} || _ <- lists:seq(1, NumNodes)] ++
                  [{map, Fun}],
       {ok, Pipe, Feed} = kflow_pipe:start_link(#{ id         => [test_pipe]
                                                 , definition => PipeDefn
                                                 }),
       ?tp(start_benchmark, #{}),
       feed_pipe(Feed, Messages),
       ?block_until(#{?snk_kind := complete}, infinity, infinity)
     end,
     %% Check stage:
     fun(_, Trace) ->
         [{pair, #{ts := T1}, #{ts := T2}}] =
           ?find_pairs( true
                      , #{?snk_kind := start_benchmark}
                      , #{?snk_kind := complete}
                      , Trace
                      ),
         DT = erlang:convert_time_unit(T2 - T1, native, millisecond),
         MPS = NMessages/DT,
         snabbkaffe:push_stat(throughput, NumNodes + 1, MPS*1000),
         true
     end).

t_throughput(Config) when is_list(Config) ->
  ?run_prop(Config, throughput_bench()).

%%====================================================================
%% Proper generators
%%====================================================================

filter_or_map() ->
  frequency([ {1, {map, kflow_test_map, #{}}}
            , {1, {filter, kflow_test_filter, #{}}}
            ]).

filter_or_aggregate(MaxBufferSize) ->
  ?LET(Size, range(0, MaxBufferSize),
       frequency([ {1, {aggregate, kflow_test_aggregate, #{size => Size}}}
                 , {1, {filter, kflow_test_filter, #{}}}
                 ])).

map_or_aggregate(MaxBufferSize) ->
  ?LET({Size, Onetime}, {range(0, MaxBufferSize), boolean()},
       frequency([ {2, {map, kflow_test_map, #{}}}
                 , {1, {aggregate, kflow_test_aggregate, #{ size    => Size
                                                          , onetime => Onetime
                                                          }}}
                 ])).

kfnode(MaxBufferSize) ->
  ?LET(Size, range(0, MaxBufferSize),
       frequency([ {3, {map, kflow_test_map, #{}}}
                 , {1, {aggregate, kflow_test_aggregate, #{size => Size}}}
                 , {1, {filter, kflow_test_filter, #{}}}
                 , {1, {mfd, kflow_test_filter, #{}}}
                 , {1, {unfold, kflow_test_unfold, #{}}}
                 ])).

message() ->
  noshrink(resize(5, list(range($A, $Z)))).

messages() ->
  ?SIZED(Size, resize(1 * Size, list({?default_route, message()}))).

feed_timeout(MinTimeout) ->
  frequency([ {1, infinity}
            , {1, range(MinTimeout + ?dt, (MinTimeout + ?dt) * 2)}
            ]).

%%====================================================================
%% Internal functions
%%====================================================================

%% Add sequential node IDs to the randomly generated pipe nodes:
add_node_ids(Spec) ->
  Ids = lists:seq(0, length(Spec) - 1),
  [{B, M, C #{id => Id}} || {Id, {B, M, C}} <- lists:zip(Ids, Spec)].

gen_map_spec(MaxDelay, Id) ->
  {map, kflow_test_map, #{id => Id, max_delay => MaxDelay}}.

%% Send messages to the pipe and emit trace for each message.
%% Returns offset of the last produced message.
-spec feed_pipe(kflow:entrypoint(), payload_gen:stream(_)) ->
                   kflow:offset().
feed_pipe(Feed, Stream) ->
  feed_pipe(Feed, Stream, 0).

-spec feed_pipe(kflow:entrypoint(), payload_gen:stream(_), kflow:offset()) ->
                   kflow:offset().
feed_pipe(Feed, [Data0|Cont], Offset0) ->
  Offset = Offset0 + 1,
  case Data0 of
    {ExpectedRoute, Payload} ->
      ok;
    _ ->
      ExpectedRoute = ?default_route,
      Payload = Data0
  end,
  Data = #{value => Payload, route => ExpectedRoute},
  ?tp(test_feed_message, #{ offset  => Offset
                          , payload => Data
                          , route   => ExpectedRoute
                          }),
  Feed(#kflow_msg{ offset  = Offset
                 , payload = Data
                 }),
  feed_pipe(Feed, payload_gen:next(Cont), Offset);
feed_pipe(_, ?end_of_stream, Offset) ->
  Offset.
