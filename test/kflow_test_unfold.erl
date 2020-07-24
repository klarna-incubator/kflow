-module(kflow_test_unfold).

-behavior(kflow_gen_unfold).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/1, unfold/3, terminate/1]).

init(#{id := Id} = Config) ->
  ?tp(kflow_test_init, #{ id       => Id
                        , behavior => unfold
                        }),
  Config.

unfold(Offset, Message, Cfg = #{id := Id}) ->
  ?tp(kflow_test_unfold_seen_message, #{ id      => Id
                                       , payload => Message
                                       , offset  => Offset
                                       }),
  N = maps:get(replicate, Cfg, 1),
  kflow_trace_specs:replicate(N, Message).

terminate(#{id := Id}) ->
  ?tp(kflow_test_terminate, #{id => Id}).
