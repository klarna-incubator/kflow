-module(kflow_test_aggregate).

-behavior(kflow_gen_aggregate).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/1, in/4, out/2, terminate/2]).

-record(s,
        { messages
        , last_offset
        , counter = 0
        , route
        }).

init(#{id := Id}) ->
  ?tp(kflow_test_init, #{ id       => Id
                        , behavior => aggregate
                        }),
  #s{messages = []}.

in( Offset, Message
  , #s{ messages = Msgs
      , counter  = Counter
      }
  , #{id := Id, size := Size}
  ) ->
  Flush = if length(Msgs) >= Size - 1 ->
              flush;
             true ->
              keep
          end,
  ?tp(kflow_test_aggregate_in, #{ id      => Id
                                , payload => Message
                                , offset  => Offset
                                , flush   => Flush
                                , counter => Counter
                                }),
  State = #s{ messages = [Message|Msgs]
            , last_offset = Offset
            , counter = Counter + 1
            },
  {Flush, State}.

out(State0 = #s{ messages    = Msgs
               , last_offset = Offset
               , counter     = Counter
               }
   , Config = #{id := Id}
   ) ->
  Output = lists:reverse(Msgs),
  ?tp(kflow_test_aggregate_out, #{ id      => Id
                                 , payload => Output
                                 , offset  => Offset
                                 , counter => Counter
                                 }),
  State = State0#s{messages = [], counter = Counter + 1},
  MaybeExit = case maps:get(onetime, Config, false) of
                true -> exit;
                false -> ok
              end,
  {MaybeExit, Output, State}.

terminate(_, #{id := Id}) ->
  ?tp(kflow_test_terminate, #{id => Id}).
