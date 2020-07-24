-ifndef(_KFLOW_INT_HRL_).
-define(_KFLOW_INT_HRL_, true).

-include("kflow.hrl").
-include_lib("hut/include/hut.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(cfg(Key), kflow_lib:cfg(Key)).

-define(default_brod_client, kflow_default_client).

-endif.
