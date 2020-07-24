%%%===================================================================
%%% @copyright 2019 Klarna Bank AB (publ)
%%%
%%% @doc This module assembles payloads transferred in chunks into
%%% files. Empty files aren't created.
%%%
%%% == Configuration ==
%%%
%%% User needs to specify the location where files will be created.
%%%
%%% ```
%%% #{location := file:filename_all()}
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
%%%  }
%%% '''
%%%
%%% == Error handling ==
%%%
%%% This module is designed to survive the following permutations of
%%% the messages:
%%%
%%% <ol>
%%% <li>Restart of the upstream from the beginning: `[1, 2, 3, 1, 2,
%%% 3, 4, 5]'</li>
%%%
%%% <li>Restart of the upstream in the middle of transfer: `[1, 2, 3,
%%% 2, 3, 4, 5]'</li>
%%%
%%% <li>Retransmission from the middle: `[3, 4, 5]'</li>
%%% </ol>
%%%
%%% This allows this pipe to survive restarts of the
%%% upstream. Duplicate chunks are ignored.
%%%
%%% @end
%%%===================================================================
-module(kflow_chunks_to_file).

-behavior(kflow_gen_assemble_chunks).

-export([in/6, out/2, chunk_num/1, chunk_count/1]).

-include_lib("hut/include/hut.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%% Normal transfer:
-record(s,
        { path :: file:name_all()
        , deleted :: boolean()
        }).

%%%===================================================================
%%% kflow_gen_assemble_chunks callbacks
%%%===================================================================

chunk_num(#{headers := Headers}) ->
  binary_to_integer(proplists:get_value(<<"slice_num">>, Headers)).

chunk_count(#{headers := Headers}) ->
  binary_to_integer(proplists:get_value(<<"slice_cnt">>, Headers)).

in(Offset, 1, SliceCnt, Msg, undefined, #{location := Location}) ->
  %% This is the beginning of transfer:
  #{ key     := Key
   , value   := Val
   , headers := Headers
   } = Msg,
  Path = filename:join(Location, Key),
  case is_deleted(Headers) of
    true ->
      do_delete(Path),
      #s{path = Path, deleted = true};
    false -> %% Beginning of file:
      ok = init_transfer(Path, SliceCnt, Offset),
      ok = file:write_file(Path, Val, [write]),
      #s{path = Path, deleted = false}
  end;
in(_Offset, SliceNum, _SliceCnt, Msg, State = #s{path = Path}, _Config)
  when SliceNum > 1 ->
  %% This is continuation of a transfer.
  %%
  %% TODO: Horrible hack. IO devices in Erlang are nothing like good
  %% old Linux file descriptors. They are actually processes linked to
  %% the process opening the file. KFlow doesn't guarantee lifetime of
  %% any process. Hence on each chunk we re-open file with `append'
  %% flag.
  ok = file:write_file(Path, maps:get(value, Msg), [append]),
  State.

out(State = #s{path = Path, deleted = Deleted}, _) ->
  Msg = #{ key => Path
         , deleted => Deleted
         },
  {exit, Msg, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_transfer(Path, SliceCnt, Offset) ->
  ?slog(info, #{ what      => "New file"
               , key       => Path
               , slice_cnt => SliceCnt
               , offset    => Offset
               }),
  ok = filelib:ensure_dir(Path).

do_delete(Path) ->
  ?slog(info, #{ what => "Deleted file"
               , key  => Path
               }),
  case file:delete(Path) of
    ok ->
      ok;
    {error, Err} ->
      ?slog(error, #{ what  => "Couldn't delete file"
                    , key   => Path
                    , error => Err
                    }),
      ok
  end.

is_deleted(Headers) ->
  case proplists:get_value(<<"deleted">>, Headers, <<"0">>) of
    <<"0">> -> false;
    <<"1">> -> true
  end.
