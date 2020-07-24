-ifndef(KAFKA_CT_SETUP_HRL).
-define(KAFKA_CT_SETUP_HRL, true).

-import(kflow_kafka_test_helper, [ produce/2
                                 , produce/3
                                 , produce/4
                                 , create_topic/2
                                 , get_acked_offsets/2
                                 , check_committed_offsets/2
                                 , wait_n_messages/2
                                 , wait_n_messages/3
                                 , consumer_config/0
                                 ]).

-include("kflow_test_macros.hrl").

-endif.
