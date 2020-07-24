-module(kflow_config).

-export([pipes/0]).

pipes() ->
  [test_workflow()].

test_workflow() ->
  #{ start => {kflow_dummy_source, start_link}
   , args => #{id => foo}
   }.
