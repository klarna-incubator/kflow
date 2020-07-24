-module(payload_gen_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

generate_chunks_test() ->
  Size = payload_gen:mb(15),
  Ret = payload_gen:generate_chunks( {1, Size}
                                   , payload_gen:mb(4)
                                   , fun({A, _, _}) -> size(A) end
                                   ),
  ?assertMatch(Size, lists:sum(Ret)).

check_consistency_test() ->
  Payload = {42, payload_gen:mb(15)},
  Ret = iolist_to_binary(
          payload_gen:generate_chunks( Payload
                                     , payload_gen:mb(4)
                                     , fun({A, _, _}) -> A end
                                     )),
  Fun = fun(N) ->
            try binary:at(Ret, N) of
                X -> {ok, X}
            catch
              _:_ -> undefined
            end
        end,
  payload_gen:check_consistency(Payload, 10000, Fun).

check_file_consistency_test() ->
  Payload = {42, payload_gen:mb(15)},
  FileName = atom_to_list(?FUNCTION_NAME) ++ ".dat",
  catch file:delete(FileName),
  Data = payload_gen:generate_chunks( Payload
                                    , payload_gen:mb(4)
                                    , fun({A, _, _}) -> A end
                                    ),
  file:write_file(FileName, Data),
  payload_gen:check_file_consistency(Payload, 10000, FileName).

%% Check that interleave stream conserves the number of elements:
interleave_lists_length_prop() ->
  ?FORALL(Lists, [{term(), [term()]}],
          begin
            Mishmash = payload_gen:consume( payload_gen:interleave_streams(Lists)
                                          , fun(A) -> A end
                                          ),
            ?assertEqual( length(Mishmash)
                        , lists:sum([length(L) || {_, L} <- Lists])
                        ),
            true
          end).

interleave_lists_length_test() ->
  ?assertEqual(true, proper:quickcheck(
                       proper:numtests(
                         100,
                         interleave_lists_length_prop())
                      )).

%% Check that interleave stream conserves elements (of a single
%% stream):
interleave_lists_self_prop() ->
  ?FORALL(L, [term()],
          begin
            L2 = payload_gen:consume( payload_gen:interleave_streams([{foo, L}])
                                    , fun(A) -> A end
                                    ),
            {Tags, L3} = lists:unzip(L2),
            %% Check that all stream elements are tagged as `foo'
            ?assertEqual( Tags
                        , [foo || _ <- L3]
                        ),
            %% Check that the input list was preserved as is:
            ?assertEqual(L3, L),
            true
          end).

interleave_lists_self_test() ->
  ?assertEqual(true, proper:quickcheck(
                       proper:numtests(
                         100,
                         interleave_lists_self_prop())
                      )).

proper_binary_stream_prop() ->
  ?FORALL(Streams, payload_gen:interleaved_binary_gen(16),
          begin
            {ExpectedTags, _} = lists:unzip(Streams),
            Stream = payload_gen:interleave_streams(Streams),
            {Tags, _} = lists:unzip(payload_gen:consume(Stream)),
            ?assertEqual(ExpectedTags, lists:usort(Tags)),
            true
          end).

proper_binary_stream_test() ->
  ?assertEqual(true, proper:quickcheck(
                       proper:numtests(
                         100,
                         proper_binary_stream_prop())
                      )).

proper_list_stream_prop() ->
  ?FORALL(Streams, payload_gen:interleaved_list_gen(term()),
          begin
            {ExpectedTags, _} = lists:unzip(Streams),
            Stream = payload_gen:interleave_streams(Streams),
            {Tags, _} = lists:unzip(payload_gen:consume(Stream)),
            ?assertEqual(ExpectedTags, lists:usort(Tags)),
            true
          end).

proper_list_stream_test() ->
  ?assertEqual(true, proper:quickcheck(
                       proper:numtests(
                         100,
                         proper_list_stream_prop())
                      )).
