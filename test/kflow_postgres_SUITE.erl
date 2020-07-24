-module(kflow_postgres_SUITE).

-include("kflow_int.hrl").
-include_lib("kflow/src/testbed/kflow_test_macros.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").

-compile(export_all).

-define(DO(Fun), do(fun() -> Fun end)).

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_suite(Config) ->
  kflow_kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) ->
  ok.

common_init_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config).

common_end_per_testcase(Case, Config) ->
  kflow_kafka_test_helper:common_end_per_testcase(Case, Config).

suite() -> [{timetrap, {seconds, 60}}].

%%====================================================================
%% Testcases
%%====================================================================

t_success({pipe_config, _}) -> [];
t_success(_Config) when is_list(_Config) ->
  wait_postgres(),
  State = kflow_postgres:init(config()),
  Msg = [ #{node => "foo", time => erlang:timestamp(), data => "{}"}
        , #{node => "bar", time => erlang:timestamp(), data => "{\"bar\":1;}"}
        ],
  ?assertMatch({ok, Msg}, ?DO(kflow_postgres:map(_Offset = 42, Msg, State))),
  kflow_postgres:terminate(State).

t_fail({pipe_config, _}) -> [];
t_fail(_Config) when is_list(_Config) ->
  wait_postgres(),
  State = kflow_postgres:init(config()),
  Msg = [ #{node => "foo", time => 0, data => "{}"}
        , #{node => "bar", time => erlang:timestamp(), data => "{\"bar\":1;}"}
        ],
  ?assertMatch({exit, _, _}, ?DO(kflow_postgres:map(_Offset = 42, Msg, State))),
  kflow_postgres:terminate(State).

%%====================================================================
%% Trace validation functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================

db_config() ->
  #{ host     => "localhost"
   , username => "kflow"
   , database => "postgres"
   , password => "123"
   }.

config() ->
  #{ database       => db_config()
   , table          => "node_role"
   , fields         => [node, time, data]
   , field_mappings => #{time => ts}
   , init_count     => 1
   }.

%% kflow runs user callbacks in temporary processes, which sometimes
%% leads to rather funny consequences. Testcases should emulate this.
do(Fun) ->
  Parent = self(),
  Ref = make_ref(),
  spawn(
    fun() ->
        Ret = try Fun() of
                  A -> {ok, A}
              catch
                EC:Err:Stack ->
                  {EC, Err, Stack}
              end,
        Parent ! {Ref, Ret}
    end),
  receive
    {Ref, Term} ->
      Term
  end.

wait_postgres() ->
  %% `epgsql:connect' works in a curious way: it first creates a
  %% socket gen_server process, links the caller to it, then attempts
  %% to send a command to the socket process (which crashes when there
  %% is no connection... and brings down the caller process via the
  %% link)... So we need two pairs of gloves and `trap_exit' to probe
  %% postgres without dying. P.S. Simply poking port 5432 doesn't
  %% quite work, I tried. Postgres starts listening before it's
  %% actually ready.
  ?log(notice, "Waiting for postgres", []),
  process_flag(trap_exit, true),
  ?retry(1000, 10,
         begin
           {ok, Conn} = epgsql:connect(db_config()),
           epgsql:close(Conn)
         end),
  ?log(notice, "Postgres is up", []).
