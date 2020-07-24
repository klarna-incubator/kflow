-ifndef(KFLOW_TEST_MACROS_HRL).
-define(KFLOW_TEST_MACROS_HRL, true).

-ifndef(SNK_COLLECTOR).
-define(SNK_COLLECTOR, true). % enable snabbkaffe test macros
-endif.
-include_lib("kflow/include/kflow.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("hut/include/hut.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

%%====================================================================
%% Macros
%%====================================================================

-define(test_client, kflow_test_client).
-define(KAFKA_HOST, "localhost").
-define(KAFKA_PORT, 9092).

-define(topic(TestCase), list_to_binary(atom_to_list(TestCase))).
-define(topic, ?topic(?FUNCTION_NAME)).

-define(group_id(TestCase), list_to_binary(atom_to_list(TestCase) ++ "_grp")).
-define(group_id, ?group_id(?FUNCTION_NAME)).

-endif.
