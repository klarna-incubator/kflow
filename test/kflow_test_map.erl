-module(kflow_test_map).

-behavior(kflow_gen_map).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/1, map/3, terminate/1]).

init(#{id := Id} = Config) ->
  ?tp(kflow_test_init, #{ id       => Id
                        , behavior => map
                        }),
  Config.

map(Offset, Message, Cfg = #{id := Id}) ->
  Delay = maps:get(max_delay, Cfg, 0),
  if Delay > 1 ->
      timer:sleep(rand:uniform(Delay));
     true ->
      ok
  end,
  ?tp(kflow_test_map_seen_message, #{ id        => Id
                                    , payload   => Message
                                    , offset    => Offset
                                    }),
  Message.

terminate(#{id := Id}) ->
  ?tp(kflow_test_terminate, #{id => Id}).
