-module(kflow_utils_tests).

-include_lib("eunit/include/eunit.hrl").

exec_test() ->
  True = os:find_executable("true"),
  False = os:find_executable("false"),
  ?assertMatch(0, kflow_utils:exec(True, ["--help"])),
  ?assertMatch(1, kflow_utils:exec(False, ["--help"])),
  true.
