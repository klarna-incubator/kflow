-module(kflow_test_filter).

-behavior(kflow_gen_filter).
-behavior(kflow_gen_mfd).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/1, filter/3, mfd/3, terminate/1]).

init(#{id := Id} = Config) ->
  ?tp(kflow_test_init, #{ id       => Id
                        , behavior => filter
                        }),
  Config.

filter(Offset, Message, #{id := Id}) ->
  Alive = rand:uniform() >= 0.5,
  ?tp(kflow_test_filter_message, #{ id      => Id
                                  , payload => Message
                                  , offset  => Offset
                                  , alive   => Alive
                                  }),
  Alive.

mfd(Offset, Message, #{id := Id}) ->
  Alive = rand:uniform() >= 0.5,
  ?tp(kflow_test_filter_message, #{ id      => Id
                                  , payload => Message
                                  , offset  => Offset
                                  , alive   => Alive
                                  }),
  if Alive ->
      {true, Message};
     true ->
      false
  end.

terminate(#{id := Id}) ->
  ?tp(kflow_test_terminate, #{id => Id}).
