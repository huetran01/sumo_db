%%% @doc Main interface for stores.
%%%
%%% Copyright 2012 Inaka &lt;hello@inaka.net&gt;
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @end
%%% @copyright Inaka <hello@inaka.net>
%%%
-module(sumo_store).
-author("Marcelo Gornstein <marcelog@gmail.com>").
-github("https://github.com/inaka").
-license("Apache License 2.0").

-behaviour(gen_server).
-define(SERVER, ?MODULE).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% Public API.
% -export([start_link/3]).
-export([start_link/1]).
-export([create_schema/2]).
-export([persist/2, async_persist/2, persist/3, async_persist/3]).
-export([delete_by/3, async_delete_by/3, delete_all/2, async_delete_all/2]).
-export([find_all/2, find_all/5, find_by/3, find_by/5, find_by/6, find_by/7]).
-export([call/4]).

%%% Exports for gen_server
-export([
  init/1, handle_call/3, handle_cast/2, handle_info/2,
  terminate/2, code_change/3
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include("sumo.hrl").

-record(state, {
  handler = undefined:: module(),
  handler_state = undefined:: any(),
  timeout = 5000 :: integer()
}).

-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Callback
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type result(R, S) :: {ok, R, S} | {error, term(), S}.
-type result(S) :: {ok, S} | {error, term(), S}.
-type affected_rows() :: unknown | non_neg_integer().

-export_type([result/2, result/1, affected_rows/0]).

-callback init(term()) -> {ok, term()}.
-callback persist(sumo_internal:doc(), State) ->
            result(sumo_internal:doc(), State).
-callback delete_by(sumo:schema_name(), sumo:conditions(), State) ->
            result(affected_rows(), State).
-callback delete_all(sumo:schema_name(), State) ->
            result(affected_rows(), State).
-callback find_by(sumo:schema_name(), sumo:conditions(), State) ->
            result([sumo_internal:doc()], State).
-callback find_by(sumo:schema_name(), sumo:conditions(), non_neg_integer(),
                  non_neg_integer(), State) ->
            result([sumo_internal:doc()], State).
-callback find_by(sumo:schema_name(), sumo:conditions(), sumo:sort(),
                  non_neg_integer(), non_neg_integer(), State) ->
            result([sumo_internal:doc()], State).
-callback find_by(sumo:schema_name(), sumo:conditions(), binary(), sumo:sort(),
                  non_neg_integer(), non_neg_integer(), State) ->
            result([sumo_internal:doc()], State).          
-callback find_all(sumo:schema_name(), State) ->
            result([sumo_internal:doc()], State).
-callback find_all(sumo:schema_name(), sumo:sort(), non_neg_integer(),
                   non_neg_integer(), State) ->
            result([sumo_internal:doc()], State).
-callback create_schema(sumo:schema(), State) -> result(State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts and links a new process for the given store implementation.
-spec start_link([term()]) -> {ok, pid()}.
start_link(Stores) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Stores], []).

%% @doc Creates the schema of the given docs in the given store name.
-spec create_schema(atom(), sumo:schema()) -> ok | {error, term()}.
create_schema(Name, Schema) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout} ->
    wpool:call(?WRITE, {create_schema, Schema, HState, Handler}, ?STRATEGY, Timeout) ;
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {create_schema, Schema}).

%% @doc Persist the given doc with the given store name.
-spec persist(
  atom(), sumo_internal:doc()
) -> {ok, sumo_internal:doc()} | {error, term()}.
persist(Name, Doc) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    wpool:call(?WRITE, {persist, Doc, HState, Handler}, ?STRATEGY, Timeout);
  Reason ->
    {error, Reason}
  end. 

-spec persist(
  atom(), binary(), sumo_internal:doc()
) -> {ok, sumo_internal:doc()} | {error, term()}.
persist(Name, Key, Doc) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    case  get(Key) of 
    undefined ->  
      wpool:call(?WRITE, {persist, Doc, HState, Handler}, ?STRATEGY, Timeout);
    OldObj -> 
      wpool:call(?WRITE, {persist, OldObj, Doc, HState, Handler}, ?STRATEGY, Timeout)
    end;
  Reason ->
    {error, Reason}
  end. 

% wpool:call(Name, {persist, Doc}).

-spec async_persist(atom(), sumo_internal:doc()) -> ok.
async_persist(Name, Doc) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState}  ->
    wpool:cast(?WRITE, {persist, Doc, HState, Handler});
  Reason ->
    {error, Reason}
  end.

-spec async_persist(atom(), binary(), sumo_internal:doc()) -> ok.
async_persist(Name, Key, Doc) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState}  ->
    case get(Key) of 
    undefined -> 
      wpool:cast(?WRITE, {persist, Doc, HState, Handler});
    OldObj -> 
      wpool:cast(?WRITE, {persist, OldObj, Doc, HState, Handler})
    end;
  Reason ->
    {error, Reason}
  end.


%% @doc Deletes the docs identified by the given conditions.
-spec delete_by(
  atom(), sumo:schema_name(), sumo:conditions()
) -> {ok, non_neg_integer()} | {error, term()}.
delete_by(Name, DocName, Conditions) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    wpool:call(?WRITE, {delete_by, DocName, Conditions, HState, Handler}, ?STRATEGY, infinity);
  Reason ->
    {error, Reason}
  end. 
  % wpool:call(Name, {delete_by, DocName, Conditions}).
-spec async_delete_by(atom(), sumo:schema_name(), sumo:conditions()) -> 
                                                            ok | {error, term()}.
async_delete_by(Name, DocName, Conditions) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState}  ->
    wpool:cast(?WRITE, {delete_by, DocName, Conditions, HState, Handler});
  Reason ->
    {error, Reason}
  end. 

%% @doc Deletes all docs in the given store name.
-spec delete_all(
  atom(), sumo:schema_name()
) -> {ok, non_neg_integer()} | {error, term()}.
delete_all(Name, DocName) ->
  case get_state(Name) of 
   #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    wpool:call(?WRITE, {delete_all, DocName, HState, Handler}, ?STRATEGY, infinity);
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {delete_all, DocName}).

%% @doc Deletes all docs in the given store name.
-spec async_delete_all(
  atom(), sumo:schema_name()
) -> ok | {error, term()}.
async_delete_all(Name, DocName) ->
  case get_state(Name) of 
   #state{handler = Handler, handler_state = HState}  ->
    wpool:cast(?WRITE, {delete_all, DocName, HState, Handler});
  Reason ->
    {error, Reason}
  end.

%% @doc Returns all docs from the given store name.
-spec find_all(
  atom(), sumo:schema_name()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_all(Name, DocName) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}   ->
    wpool:call(?READ, {find_all, DocName, HState, Handler}, ?STRATEGY, Timeout);
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {find_all, DocName}).

%% @doc Returns Limit docs starting at Offset from the given store name,
%% ordered by OrderField. OrderField may be 'undefined'.
-spec find_all(
  atom(), sumo:schema_name(), sumo:sort(),
  non_neg_integer(), non_neg_integer()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_all(Name, DocName, SortFields, Limit, Offset) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    Reply = wpool:call(?READ, {find_all, DocName, SortFields, Limit, Offset, HState, Handler}, ?STRATEGY, Timeout),
    handle_reply(DocName, Reply);
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {find_all, DocName, SortFields, Limit, Offset}).

%% @doc Finds documents that match the given conditions in the given
%% store name.
-spec find_by(
  atom(), sumo:schema_name(), sumo:conditions()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_by(Name, DocName, Conditions) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    Reply = wpool:call(?READ, {find_by, DocName, Conditions, HState, Handler}, ?STRATEGY, Timeout),
    handle_reply(DocName, Reply);
    % handle_find_by(DocName, Conditions, State);
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {find_by, DocName, Conditions}).

%% @doc Finds documents that match the given conditions in the given
%% store name.
-spec find_by(
  atom(), sumo:schema_name(), sumo:conditions(),
  non_neg_integer(), non_neg_integer()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_by(Name, DocName, Conditions, Limit, Offset) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    Reply = wpool:call(?READ, {find_by, DocName, Conditions, Limit, Offset, HState, Handler}, ?STRATEGY, Timeout),
    handle_reply(DocName, Reply);
  Reason ->
    {error, Reason}
  end.
  % wpool:call(Name, {find_by, DocName, Conditions, Limit, Offset}).

%% @doc Finds documents that match the given conditions in the given
%% store name.
-spec find_by(
  atom(), sumo:schema_name(), sumo:conditions(),
  sumo:sort(), non_neg_integer(), non_neg_integer()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_by(Name, DocName, Conditions, SortFields, Limit, Offset) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    Reply= wpool:call(?READ, {find_by, DocName, Conditions, SortFields, Limit, Offset, HState, Handler}, ?STRATEGY, Timeout),
    handle_reply(DocName, Reply);
  Reason ->
    {error, Reason}
  end.

-spec find_by(
  atom(), sumo:schema_name(), sumo:conditions(), binary(),
  sumo:sort(), non_neg_integer(), non_neg_integer()
) -> {ok, [sumo_internal:doc()]} | {error, term()}.
find_by(Name, DocName, Conditions, Filter, SortFields, Limit, Offset) ->
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    Reply = wpool:call(?READ, {find_by, DocName, Conditions, Filter, SortFields, Limit, Offset, HState, Handler}, ?STRATEGY, Timeout),
    handle_reply(DocName, Reply);
  Reason ->
    {error, Reason}
  end.

%% @doc Calls a custom function in the given store name.
-spec call(
  atom(), sumo:schema_name(), atom(), [term()]
) -> ok | {ok, term()} | {error, term()}.
call(Name, DocName, Function, Args) ->
  % {ok, Timeout} = application:get_env(sumo_db, query_timeout),
  case get_state(Name) of 
  #state{handler = Handler, handler_state = HState, timeout = Timeout}  ->
    wpool:call(?WRITE, {call, Handler, Function, Args, DocName, HState}, ?STRATEGY, Timeout);
  Reason ->
    {error, Reason}
  end.

handle_reply(_DocName, {ok, []} = Reply) -> Reply;
handle_reply(DocName, {ok, DataResp}) ->
  NewDataResp = 
  lists:map(fun
    (#{doc := Doc, obj := Obj}) ->
      DocField = sumo_internal:doc_fields(Doc),
      IdField = sumo_internal:id_field_name(DocName),
      Key = maps:get(IdField, DocField),
      put(Key, Obj),
      Doc;
    (Resp) -> Resp 
  end, DataResp),
  {ok, NewDataResp};
handle_reply(_DocName, Reply) -> Reply.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server stuff.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Called by start_link.
%% @hidden
% -spec init([term()]) -> {ok, state()}.
% init([Module, Options]) ->
%   {ok, HState} = Module:init(Options),
%   {ok, #state{handler=Module, handler_state=HState}}.
-spec init(list()) -> {ok, #state{}}.
init([Stores]) ->
  Timeout = application:get_env(sumo_db, query_timeout, 5000),
  Options = lists:map(fun({Name, Module, Options}) ->  
    {ok, HState} = Module:init(Options),
    {Name, #state{handler=Module, handler_state=HState, timeout = Timeout}}
  end, Stores),
  sumo_store_opts:init(Options),
  {ok, #state{}}.


%% @doc handles calls.
%% @hidden
% -spec handle_call(term(), _, state()) -> {reply, tuple(), state()}.
% handle_call(
%   {persist, Doc}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:persist(Doc, HState),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {delete_by, DocName, Conditions}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:delete_by(DocName, Conditions, HState),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {delete_all, DocName}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:delete_all(DocName, HState),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {find_all, DocName}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_all(DocName, HState),
%   {reply, {OkOrError, Reply}, State#state{handler_state = NewState}};


% handle_call(
%   {find_all, DocName, SortFields, Limit, Offset}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_all(
%     DocName, SortFields, Limit, Offset, HState
%   ),
%   {reply, {OkOrError, Reply}, State#state{handler_state = NewState}};


% handle_call(
%   {find_by, DocName, Conditions}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_by(DocName, Conditions, HState),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {find_by, DocName, Conditions, Limit, Offset}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_by(
%     DocName, Conditions, Limit, Offset, HState
%   ),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {find_by, DocName, Conditions, SortFields, Limit, Offset}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_by(
%     DocName, Conditions, SortFields, Limit, Offset, HState
%   ),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {find_by, DocName, Conditions, Filter, SortFields, Limit, Offset}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {OkOrError, Reply, NewState} = Handler:find_by(
%     DocName, Conditions, Filter, SortFields, Limit, Offset, HState
%   ),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {call, DocName, Function, Args}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   RealArgs = lists:append(Args, [DocName, HState]),
%   {OkOrError, Reply, NewState} = erlang:apply(Handler, Function, RealArgs),
%   {reply, {OkOrError, Reply}, State#state{handler_state=NewState}};


% handle_call(
%   {create_schema, Schema}, _From,
%   #state{handler = Handler, handler_state = HState} = State
% ) ->
%   {Result, NewState} = case Handler:create_schema(Schema, HState) of
%     {ok, NewState_} -> {ok, NewState_};
%     {error, Error, NewState_} -> {{error, Error}, NewState_}
%   end,
%   {reply, Result, State#state{handler_state=NewState}}.


get_state(Name) ->
  catch sumo_store_opts:get_key(Name).

%% @hidden
-spec handle_call(term(), term(), state()) -> 
      {reply, term(), state()}.
handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

-spec handle_cast(term(), state()) ->
  {noreply, state()}
  | {noreply, state(), non_neg_integer()}
  | {noreply, state(), hibernate}
  | {stop, term(), state()}.
handle_cast(_Msg, State) ->
  {noreply, State}.

%% @hidden
-spec handle_info(term(), state()) ->
  {noreply, state()}
  | {noreply, state(), non_neg_integer()}
  | {noreply, state(), hibernate}
  | {stop, term(), state()}.
handle_info(_Info, State) ->
  {noreply, State}.

%% @hidden
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
  ok.

%% @hidden
-spec code_change(term(), state(), term()) ->
  {ok, state()} | {error, term()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
