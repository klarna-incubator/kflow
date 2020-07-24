-ifndef(_KFLOW_HRL_).
-define(_KFLOW_HRL_, true).

-record(kflow_msg,
        { offset                 :: integer()
        , fully_processed_offset :: integer() | undefined
        , hidden = false         :: boolean()
        , payload                :: term()
        , route = []             :: kflow:route()
        }).

-record(init_data,
        { id                    :: kflow:node_id()
        , cb_module             :: module()
        , config                :: term()
        , cb_timeout = infinity :: timeout()
        , max_queue_len = 1     :: non_neg_integer()
        }).

-endif.
