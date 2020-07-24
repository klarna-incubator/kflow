%%%===================================================================
%%% @copyright 2018-2020 Klarna Bank AB (publ)
%%%
%%% @doc This module inserts a batch of Erlang maps as rows in a
%%% Postgres table.
%%%
%%% == Input message format ==
%%%
%%% ```
%%% [ #{field1 => val1, field2 => val2}
%%% , #{field1 => val3, field2 => val4}
%%% ]
%%% '''
%%%
%%% == Output message format ==
%%%
%%% Input message is returned as is.
%%%
%%% == Configuration ==
%%%
%%% Configuration is a term of {@link config/0} type. It's a map with
%%% the following mandatory fields:
%%%
%%% <ul><li>`database' PostgreSQL connection options, see
%%% [https://github.com/epgsql/epgsql/blob/devel/src/epgsql.erl#L49]</li>
%%%
%%% <li>`table' is name of Postgres table used for storing
%%% data</li>
%%%
%%% <li>`fields' is a list of map keys that should be inserted to the
%%% table. By default, table column with the name matching will be
%%% used</li></ul>
%%%
%%% The following options are optional:
%%%
%%% <ul><li>`field_mappings' allows to specify column names used for
%%% the keys. This value should be a map `#{Key => ColumnName}'</li>
%%%
%%% <li>`keys' parameter allows to specify which columens are primary
%%% keys in the table. `kflow_postgres' will overwrite row with
%%% colliding keys via upsert.</li></ul>
%%%
%%% == Example usage ==
%%%
%%% ```
%%% {map, kflow_postgres, #{ database => #{ host     => "localhost"
%%%                                       , user     => "kflow"
%%%                                       , password => "123"
%%%                                       , database => "postgres"
%%%                                       , port     => 5432
%%%                                       }
%%%                        , table  => "foo_table"
%%%                        , fields => [foo, bar, baz]
%%%                        , keys   => [foo]
%%%                        }}'''
%%%
%%% @end
%%%===================================================================
-module(kflow_postgres).

-behavior(kflow_gen_map).

-include_lib("hut/include/hut.hrl").

%% Pipe callbacks:
-export([ init/1
        , terminate/1
        , map/3
        ]).

-export_type([config/0]).

%%%===================================================================
%%% Types
%%%===================================================================

-type field() :: atom() | list() | binary().

-type field_mappings() :: #{field() => field()}.

-record(s,
        { connection    :: pid()
        , sql_statement :: iolist()
        , fields        :: [field()]
        }).

-type config() ::
   #{ database         := epgsql:connect_opts()
    , init_count       => non_neg_integer()
    , fields           := [field()]
    , keys             => [field()]
    , table            := list() | binary()
    , field_mappings   => field_mappings()
    }.

%%%===================================================================
%%% kflow_gen_map callbacks
%%%===================================================================

%% @private
init(Config = #{fields := Fields}) ->
  {ok, Conn} = epgsql:connect(epgsql_config(Config)),
  Statement = mk_statement(Config),
  kflow_lib:redirect_logs(Conn),
  ?log(notice, "Generated statement: ~s", [Statement]),
  #s{ connection    = Conn
    , sql_statement = Statement
    , fields        = Fields
    }.

%% @private
terminate(#s{connection = Conn}) ->
  epgsql:close(Conn).

%% @private
map(Offset, Msg, S) ->
  Data = [transform(Offset, I, S) || I <- Msg],
  pg_store(Data, S),
  Msg.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec transform(kflow:offset(), map() | list(), #s{}) -> [term()].
transform(Offset, Data0, S) ->
  Data1 = if is_list(Data0) ->
              maps:from_list(Data0);
             is_map(Data0) ->
              Data0
          end,
  Data = Data1#{'_offset' => Offset},
  [ begin
      case Data of
        #{Key := Val} ->
          Val;
        _->
          ?slog(critical, #{ what  => "Missing data for a record field"
                           , data  => Data
                           , field => Key
                           }),
          error(missing_data)
      end
    end
    || Key <- S#s.fields].

%% @private
-spec mk_statement(config()) -> epgsql:sql_query().
mk_statement(#{ table  := Table
              , fields := Fields
              } = Config) ->
  Mappings = maps:get(field_mappings, Config, #{}),
  Keys0 = maps:get(keys, Config, []),
  FCol0 = [field_to_sql(maps:get(Field, Mappings, Field))
           || Field <- Fields],
  Keys = [field_to_sql(maps:get(Field, Mappings, Field))
          || Field <- Keys0],
  FCol = string:join(FCol0, ", "),
  SCol0 = [lists:flatten(io_lib:format("$~p" , [I]))
           || I <- lists:seq(1, length(Fields))],
  SCol = string:join(SCol0, ", "),
  [ "INSERT INTO ", Table, " (", FCol, ") "
  , "VALUES (", SCol, ")"
  | upserts(FCol0, Keys)
  ].

%% @private
upserts(Fields, Keys) ->
  case Keys of
    [] ->
      [];
    _ ->
      Keys1 = string:join(Keys, ", "),
      [ " ON CONFLICT (", Keys1, ") DO UPDATE SET "
      | string:join( [ lists:flatten([Field, " = EXCLUDED.", Field])
                       || Field <- Fields -- Keys]
                   , ", "
                   )
      ]
  end.

%% @private
-spec epgsql_config(config()) -> epgsql:connect_options().
epgsql_config(#{database := ConnCfg0}) ->
  ConnCfg1 = if is_map(ConnCfg0)  -> maps:to_list(ConnCfg0);
                is_list(ConnCfg0) -> ConnCfg0
             end,
  %% This hides the password from the logs:
  case lists:keyfind(password, 1, ConnCfg1) of
    {password, Psw} when not is_function(Psw, 0) ->
      lists:keyreplace( password
                      , 1
                      , ConnCfg1
                      , {password, fun() -> Psw end}
                      );
    _ -> ConnCfg1
  end.

%% @private Insert a batch of values into the table, using a prepared
%% statements and check that each and every record was indeed inserted
-spec pg_store([list()], #s{}) -> ok.
pg_store(Data, #s{connection = Conn, sql_statement = Statement0}) ->
  {ok, Statement} = epgsql:parse(Conn, Statement0),
  Batch = [{Statement, I} || I <- Data],
  Result = epgsql:execute_batch(Conn, Batch),
  lists:foreach( fun(Ret) -> {ok, _} = Ret end
               , Result
               ).

%%% @private Encode Erlang term as column name
-spec field_to_sql(field()) -> list().
field_to_sql(Bin) when is_binary(Bin) ->
  binary_to_list(Bin);
field_to_sql(Atom) when is_atom(Atom) ->
  atom_to_list(Atom);
field_to_sql(String) ->
  true = io_lib:printable_list(String),
  String.
