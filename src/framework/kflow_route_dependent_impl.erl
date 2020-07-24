%%% @copyright 2020 Klarna Bank AB (publ)
%%%
%%% @private
-module(kflow_route_dependent_impl).

-behavior(kflow_multistate).

-include("kflow.hrl").
-include_lib("hut/include/hut.hrl").

-export([ init/3
        , handle_message/3
        , handle_flush/2
        , terminate/2
        , fully_processed_offset/1
        ]).

-record(s,
        { icbm  :: module() %% Intermediate CallBack Module
        , state :: term()
        , cfg   :: term()
        }).

%% @private
init(NodeId, [RouteHd|_], GenNodeFun) ->
  NodeConfig = GenNodeFun(RouteHd),
  {ICBM, _NodeConfig, ICBMConfig} = kflow_pipe:desugar_node_spec(NodeConfig),
  {ok, State} = ICBM:init(NodeId, ICBMConfig),
  {ok, #s{ icbm  = ICBM
         , cfg   = ICBMConfig
         , state = State
         }}.

%% @private
handle_message(Msg, State = #s{icbm = ICBM, state = S, cfg = Cfg}, _) ->
  %% Matching with `S' is intentional. Did I mention that it only
  %% works for stateless ICBMs?
  {Ret, Msgs, S} = ICBM:handle_message(Msg, S, Cfg),
  {Ret, Msgs, State}.

%% @private
handle_flush(State = #s{icbm = ICBM, state = S, cfg = Cfg}, _) ->
  %% Matching with `S' is intentional. Did I mention that it only
  %% works for stateless ICBMs?
  {Ret, Msgs, S} = ICBM:handle_flush(S, Cfg),
  {Ret, Msgs, State}.

%% @private
terminate(#s{icbm = ICBM, state = S, cfg = Cfg}, _) ->
  ICBM:terminate(S, Cfg).

%% @private
fully_processed_offset(_) ->
  undefined.
