%%%===================================================================
%%% @copyright 2020 Klarna Bank AB (publ)
%%%
%%% @doc Miscellaneous functions that can be useful for implementing
%%% workflows.
%%%
%%% @end
%%%===================================================================
-module(kflow_utils).

-include_lib("hut/include/hut.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-export([id/1, k/2, exec/2, upload_to_s3/3, aws_config/0, ensure_string/1,
        retry/3, retry/2]).

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Identity function (useful as a placeholder)
-spec id(A) -> A.
id(A) ->
  A.

%% @doc K combinator (useful as a placeholder)
-spec k(A, _B) -> A.
k(A, _) ->
  A.

%% @doc Execute an external executable `Executable' with args `Args'
%% and return the exit status
-spec exec(file:filename(), [string() | binary()]) -> integer().
exec(CMD, Args) ->
  Port = open_port( {spawn_executable, CMD}
                  , [ exit_status
                    , binary
                    , stderr_to_stdout
                    , {args, Args}
                    ]
                  ),
  ?log(debug, "port_command ~p: ~s ~p", [Port, CMD, Args]),
  collect_port_output(Port).

-spec ensure_string(string() | binary()) -> string().
ensure_string(Str) when is_list(Str) ->
  Str;
ensure_string(Bin) when is_binary(Bin) ->
  binary_to_list(Bin).

-spec upload_to_s3(string(), string(), binary()) -> ok.
upload_to_s3(Bucket, Key, Value) ->
  {ok, Headers} = application:get_env(kflow, s3_headers),
  {ok, Options} = application:get_env(kflow, s3_options),
  Args = [Bucket, Key, Value, Options, Headers, kflow_utils:aws_config()],
  _ = retry({erlcloud_s3, put_object, Args}, 2),
  ok.

-spec aws_config() -> #aws_config{}.
aws_config() ->
  AwsConfig0 = erlcloud_aws:default_config(),
  AwsConfig0#aws_config{s3_follow_redirect = true}.

-spec retry({module(), atom(), list()}, non_neg_integer(), non_neg_integer()) -> _.
retry({M, F, A}, 0, _) ->
  apply(M, F, A);
retry(MFA = {M, F, A}, N, Timeout) ->
  try
    apply(M, F, A)
  catch
    _:_ ->
      ?log(warning, "Retrying ~p:~p/~p", [M, F, length(A)]),
      timer:sleep(Timeout),
      retry(MFA, N - 1, Timeout)
  end.

-spec retry({module(), atom(), list()}, non_neg_integer()) -> _.
retry(Fun, N) ->
  retry(Fun, N, 3000).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec collect_port_output(port()) -> integer().
collect_port_output(Port) ->
  %% TODO: outputs of commands running in parallel may get mixed
  %% together, do something about this.
  receive
    {Port, {data, Data}} ->
      ?log(info, "Port=~p~n~s", [Port, Data]),
      collect_port_output(Port);
    {Port, {exit_status, ExitStatus}} ->
      ExitStatus
  end.
