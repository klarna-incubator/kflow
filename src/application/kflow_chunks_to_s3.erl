%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module assembles payloads transferred in chunks into S3
%%% objects. Warning: chunks should be at least 5MB in size, due to
%%% limitations of AWS S3 multipart upload API.
%%%
%%% Note: this module aborts multipart uploads in a best effort
%%% way. It's still necessary to configure incomplete multipart upload
%%% lifecycle to avoid lingering uploads.
%%%
%%% == Configuration ==
%%%
%%% User needs to specify the bucket where objects will be created:
%%%
%%% ```
%%% #{ s3_bucket := string()
%%%  , prefix    => binary()
%%%  }
%%% '''
%%%
%%% == Input message format ==
%%%
%%% ```
%%% #{ key             := binary()
%%%  , value           := binary()
%%%  , headers := [{<<"slice_cnt">>, binary()}, % e.g. <<"42">>
%%%               ,{<<"slice_num">>, binary()} % e.g. <<"1">> (1-based)
%%%               ,{<<"deleted">>,<<"0">> | <<"1">>}
%%%               ]
%%%  , ...
%%%  }
%%% '''
%%%
%%% == Output message format ==
%%%
%%%
%%% ```
%%% #{ key            := binary()
%%%  , retransmission := boolean()
%%%  , deleted        := boolean()
%%%  , ...
%%%  }
%%% '''
%%%
%%% @end
%%%===================================================================
-module(kflow_chunks_to_s3).

-behavior(kflow_gen_assemble_chunks).

%% callbacks:
-export([terminate/2, in/6, out/2, chunk_num/1, chunk_count/1]).

-include_lib("hut/include/hut.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%% Normal transfer:
-record(s,
        { key                :: file:name_all()
        , deleted = false    :: boolean()
        , upload_id          :: string() | undefined
        , etags = []         :: list()
        }).

-type state() :: undefined | #s{}.

-type config() ::
        #{ s3_bucket := string()
         , prefix    => binary()
         }.

%%%===================================================================
%%% kflow_gen_assemble_chunks callbacks
%%%===================================================================

%% @private
chunk_num(#{headers := Headers}) ->
  binary_to_integer(proplists:get_value(<<"slice_num">>, Headers)).

%% @private
chunk_count(#{headers := Headers}) ->
  binary_to_integer(proplists:get_value(<<"slice_cnt">>, Headers)).

%% @private
-spec in(kflow:offset(), integer(), integer(), map(), state(), config()) -> state().
in(_Offset, N, Nmax, Msg = #{key := Key0, value := Value}, State0, Config) ->
  Deleted = is_deleted(Msg),
  Key = s3_key(Key0, Config),
  if Deleted ->
      handle_delete(Key, Config);
     Nmax =:= 1 ->
      handle_singleton(Key, Value, Config);
     N =:= 1 ->
      State = handle_begin_transfer(Key, Config),
      upload_chunk(1, Key, Value, State, Config);
     true ->
      upload_chunk(N, Key, Value, State0, Config)
  end.

%% @private
-spec out(state(), config()) -> {exit, map(), state()}.
out(State, #{s3_bucket := Bucket}) ->
  #s{ upload_id = UID
    , etags     = ETags0
    , key       = Key
    , deleted   = Deleted
    } = State,
  case UID of
    undefined ->
      ok;
    _ ->
      ETags = lists:reverse(ETags0),
      Headers = [],
      erlcloud_s3:complete_multipart(Bucket, Key, UID, ETags, Headers,
                                     kflow_utils:aws_config())
  end,
  Msg = #{key => Key, deleted => Deleted},
  {exit, Msg, undefined}.

%% @private
-spec terminate(state(), config()) -> ok.
terminate(undefined, _) ->
  ok;
terminate(#s{key = Key, upload_id = UID}, #{s3_bucket := Bucket}) ->
  erlcloud_s3:abort_multipart(Bucket, Key, UID, [], [],
                              kflow_utils:aws_config()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_singleton(string(), binary(), config()) -> state().
handle_singleton(Key, Value, #{s3_bucket := Bucket}) ->
  ?slog(info, #{ what => "Uploading a small object"
               , key  => Key
               }),
  ok = kflow_utils:upload_to_s3(Bucket, Key, Value),
  #s{key = Key, deleted = false}.

-spec handle_delete(string(), config()) -> state().
handle_delete(Key, #{s3_bucket := Bucket}) ->
  erlcloud_s3:delete_object(Bucket, Key, kflow_utils:aws_config()),
  #s{key = Key, deleted = true}.

-spec handle_begin_transfer(string(), config()) -> state().
handle_begin_transfer(Key, #{s3_bucket := Bucket}) ->
  {ok, Headers} = application:get_env(kflow, s3_headers),
  {ok, Options} = application:get_env(kflow, s3_options),
  {ok, Response} = erlcloud_s3:start_multipart(Bucket, Key, Options, Headers,
                                               kflow_utils:aws_config()),
  {uploadId, UploadID} = lists:keyfind(uploadId, 1, Response),
  #s{upload_id = UploadID, key = Key, deleted = false}.

-spec upload_chunk(integer(), string(), binary(), state(), config()) -> state().
upload_chunk(N, Key, Value, State0, #{s3_bucket := Bucket}) ->
  #s{upload_id = UID, etags = ETags0} = State0,
  {ok, Return} = erlcloud_s3:upload_part(Bucket, Key, UID, N, Value, [],
                                         kflow_utils:aws_config()),
  {etag, ETag} = lists:keyfind(etag, 1, Return),
  Etags = [{N, ETag} | ETags0],
  State0#s{etags = Etags}.

-spec s3_key(binary(), config()) -> string().
s3_key(Key, #{prefix := Pre}) ->
  binary_to_list(iolist_to_binary([Pre, $/, Key]));
s3_key(Key, _) ->
  binary_to_list(Key).

-spec is_deleted(map()) -> boolean().
is_deleted(#{headers := Headers}) ->
  case proplists:get_value(<<"deleted">>, Headers, <<"0">>) of
    <<"0">> -> false;
    <<"1">> -> true
  end.
