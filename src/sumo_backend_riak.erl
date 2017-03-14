%%% @hidden
%%% @doc Riak storage backend implementation.
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
-module(sumo_backend_riak).
-author("Carlos Andres Bolanos <candres.bolanos@inakanetworks.com>").
-license("Apache License 2.0").

-behaviour(gen_server).
-behaviour(sumo_backend).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% Public API.
-export([get_connection/1,
	get_conn_info/1,
	pool_name/1,
	statistic/1]).

%%% Exports for sumo_backend
-export([start_link/2]).

%%% Exports for gen_server
-export([ init/1
		, handle_call/3
		, handle_cast/2
		, handle_info/2
		, terminate/2
		, code_change/3
		]).

-export([create_schema/3
		,persist/3
		,persist/4
		,delete_by/4
		,delete_all/3
		,find_all/3
		,find_all/6
		,find_by/4
		,find_by/6
		,find_by/7
		,find_by/8
		,call/5]).

%% Debug
-export([get_riak_conn/1]).

-include_lib("riakc/include/riakc.hrl").
-include("sumo.hrl").

-define(THROW_TO_ERROR(X), try X catch throw:Result -> erlang:raise(error, Result, erlang:get_stacktrace()) end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(modstate, {host :: string(),
				port :: non_neg_integer(),
				opts :: [term()],
				pool_name :: binary(),
				conn :: connection(),
				worker_handler :: pid(), 
				timeout_read :: integer(),
				timeout_write :: integer(),
				timeout_mapreduce :: integer(),
				auto_reconnect :: boolean()}).

-record(state, {conn :: connection(),
		bucket   :: bucket(),
		index    :: index(),
		get_opts :: get_options(),
		put_opts :: put_options(),
		del_opts :: delete_options()}).

-type state() :: #modstate{}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(atom(), proplists:proplist()) -> {ok, pid()}|term().
start_link(Name, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, Options, []).

-spec get_connection(atom() | pid()) -> atom().
get_connection(Name) ->
  gen_server:call(Name, get_connection).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server stuff.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init([term()]) -> {ok, pid()}.
init([undefined, State]) ->
  
  % {ok, Conn} = riakc_pb_socket:start_link(Host, Port, [{auto_reconnect, AutoReconnect}]),
  % ets:insert(sumo_pool, {PoolName, Conn}),
  HandlerPid = spawn_link(fun() -> worker_init(State) end),
  HandlerPid ! {init_conn, self()},
  {ok, State#modstate{worker_handler = HandlerPid}};

init(Options) ->
  %% Get connection parameters
  Host = proplists:get_value(host, Options, "127.0.0.1"),
  Port = proplists:get_value(port, Options, 8087),
  PoolSize = proplists:get_value(poolsize, Options, 50),
  WritePoolSize = proplists:get_value(write_pool_size, Options, PoolSize),
  _ReadPoolSize = proplists:get_value(read_pool_size, Options, 50),
  TimeoutRead = proplists:get_value(timeout_read, Options,  ?TIMEOUT_GENERAL),
  TimeoutWrite = proplists:get_value(timeout_write, Options, ?TIMEOUT_GENERAL),
  TimeoutMapReduce = proplists:get_value(timeout_mapreduce, Options, ?TIMEOUT_GENERAL),
  AutoReconnect = proplists:get_value(auto_reconnect, Options, true),
  Opts = riak_opts(Options),
  State = #modstate{host = Host, port = Port, opts = Opts, timeout_read = TimeoutRead,
			timeout_write = TimeoutWrite, timeout_mapreduce = TimeoutMapReduce,
			auto_reconnect = AutoReconnect},
  WritePoolOptions    = [ {overrun_warning, 10000}
					, {overrun_handler, {sumo_internal, report_overrun}}
					, {workers, WritePoolSize}
					, {worker, {?MODULE, [undefined, State#modstate{pool_name = ?WRITE}]}}],
  % ReadPoolOptions    = [ {overrun_warning, 10000}
		% 			, {overrun_handler, {sumo_internal, report_overrun}}
		% 			, {workers, ReadPoolSize}
		% 			, {worker, {?MODULE, [undefined, State#modstate{pool_name = ?READ}]}}],
  % ets:new(sumo_pool, [named_table, bag, public, {write_concurrency, true}, {read_concurrency, true}]),
  wpool:start_pool(?WRITE, WritePoolOptions),
  % wpool:start_pool(?READ, ReadPoolOptions),
  {ok, #modstate{host = Host, port = Port, opts = Opts}}.

%%%
%%%

create_schema(Schema, HState, Handler) ->
	wpool:call(?WRITE, {create_schema, Schema, HState, Handler}).

persist( Doc, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:persist(Doc, HState#state{conn = Conn}),
  {OkOrError, Reply}.

persist(OldObj, Doc, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:persist(OldObj, Doc, HState#state{conn = Conn}),
  {OkOrError, Reply}.

delete_by(DocName, Conditions, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:delete_by(DocName, Conditions, HState#state{conn = Conn}),
  {OkOrError, Reply}.


delete_all(DocName, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:delete_all(DocName, HState#state{conn = Conn}),
  {OkOrError, Reply}.

find_all(DocName, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_all(DocName, HState#state{conn = Conn}),
  {OkOrError, Reply}.

find_all(DocName, SortFields, Limit, Offset, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_all(DocName, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {OkOrError, Reply}.


find_by(DocName, Conditions, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, HState#state{conn = Conn}),
  {OkOrError, Reply}.

find_by(DocName, Conditions, Limit, Offset, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Limit, Offset, HState#state{conn = Conn}),
  {OkOrError, Reply}.

find_by(DocName, Conditions, SortFields, Limit, Offset, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {OkOrError, Reply}.


find_by(DocName, Conditions, Filter, SortFields, Limit, Offset, HState, Handler) ->
  Conn = get_riak_conn(?WRITE),
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Filter, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {OkOrError, Reply}.


call(Handler, Function, Args, DocName, HState) ->
  Conn = get_riak_conn(?WRITE),
  RealArgs = lists:append(Args, [DocName, HState#state{conn = Conn}]),
  {OkOrError, Reply, _} = erlang:apply(Handler, Function, RealArgs),
  {OkOrError, Reply}.



%% @todo: implement connection pool.
%% In other cases is a built-in feature of the client.
-spec handle_call(term(), term(), state()) -> {reply, term(), state()}.
handle_call(get_connection, _From, State = #modstate{host = Host, port = Port, 
									opts = Opts, pool_name = PoolName }) ->
  {ok, Conn} = riakc_pb_socket:start_link(Host, Port, Opts),
  ets:insert(sumo_pool, {PoolName, Conn}),
  {reply, Conn, State#modstate{conn = Conn}};

handle_call(get_conn_info, _From, State = #modstate{conn = Conn}) ->
  {reply, Conn, State};


%%%------------------------------------


% handle_call({create_schema, Schema, HState, Handler}, _From, Conn = State) ->
%   {Result, _} = case Handler:create_schema(Schema, HState#state{conn = Conn}) of
%   {ok, NewState_} -> {ok, NewState_};
%   {error, Error, NewState_} -> {{error, Error}, NewState_}
%   end,
%   {reply, Result, State};


% handle_call({persist, Doc, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:persist(Doc, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({persist, OldObj, Doc, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:persist(OldObj, Doc, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({delete_by, DocName, Conditions, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:delete_by(DocName, Conditions, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};


% handle_call({delete_all, DocName, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:delete_all(DocName, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};


% handle_call({find_all, DocName, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_all(DocName, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({find_all, DocName, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_all(DocName, SortFields, Limit, Offset, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({find_by, DocName, Conditions, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({find_by, DocName, Conditions, Limit, Offset, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Limit, Offset, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({find_by, DocName, Conditions, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, SortFields, Limit, Offset, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({find_by, DocName, Conditions, Filter, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
%   {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Filter, SortFields, Limit, Offset, HState#state{conn = Conn}),
%   {reply, {OkOrError, Reply}, State};

% handle_call({call, Handler, Function, Args, DocName, HState}, _From, Conn = State) ->
%   RealArgs = lists:append(Args, [DocName, HState#state{conn = Conn}]),
%   {OkOrError, Reply, _} = erlang:apply(Handler, Function, RealArgs),
%   {reply, {OkOrError, Reply}, State};

handle_call({create_schema, Schema, HState, Handler}, From, #modstate{worker_handler = HandlerPid} = State) ->
		HandlerPid ! {create_schema, From, Schema, HState, Handler},
		{reply, ok, State};

handle_call({find_key, function, Fun}, From, #modstate{worker_handler = HandlerPid} = State) ->
	HandlerPid ! {find_key, From, {function, Fun}},
	{reply, ok,  State};

handle_call(test_ok, _From,#modstate{worker_handler = HandlerPid} = State) ->
  {reply, HandlerPid, State};

handle_call(test_crash, _From, #modstate{conn = Conn} = State) ->
  %% do something forced process died 
  % A = 1, 
  % A = 2 ,
  {reply, Conn, State};

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Unused Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handle_cast({persist, Doc, HState, Handler}, Conn = State) ->
%   {_OkOrError, Reply, _} = Handler:persist(Doc, HState#state{conn = Conn}),
%   %% TODO next: should call event here
%   lager:debug("sumo: asyn_persist: Doc: ~p,  Reply: ~p ~n",[Doc, Reply]),
%   {noreply, State};

% handle_cast({persist, OldObj, Doc, HState, Handler}, Conn = State) ->
%   {_OkOrError, Reply, _} = Handler:persist(OldObj, Doc, HState#state{conn = Conn}),
%   lager:debug("sumo: asyn_persist: Doc: ~p, Reply: ~p",[Doc, Reply]),
%   {noreply, State};


% handle_cast({delete_by, DocName, Conditions, HState, Handler}, Conn = State) ->
%   {_OkOrError, Reply, _} = Handler:delete_by(DocName, Conditions, HState#state{conn = Conn}),
%   %% TODO: next : should call event here 
%   lager:debug("sumo: asyn_delete_by: Doc: ~p, Reply: ~p ~n",[DocName, Reply]),
%   {noreply, State};

% handle_cast({delete_all, DocName, HState, Handler}, Conn = State) ->
%   {_OkOrError, Reply, _} = Handler:delete_all(DocName, HState#state{conn = Conn}),
%   %% TODO: next: should call event here
%   lager:debug("sumo: async_delete_all: Doc: ~p, Reply: ~p ~n",[DocName, Reply]),
%   {noreply, State};

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) -> {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connected, State) ->
	{noreply, State};

handle_info({fail_init_conn, _Why}, State) ->
	{stop, normal, State };

handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> 
  ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server stuff.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

worker_init(State) ->
	process_flag(trap_exit, true),
	work_loop(State).


work_loop(State) ->
	Conn = State#modstate.conn,
	receive
		{init_conn, Caller} ->
			case connection(State) of 
			{ok,  ConnState} ->
				Caller ! connected,
				ConnState;
			Error ->
				Caller ! {fail_init_conn, Error},
				State
			end,
			work_loop(State);
		{find_key, Caller, {function, Fun}} ->
			Fun(Conn),
			work_loop(State);
		{create_schema, Caller,  Schema, HState, Handler} ->
			handle_create_schema(Schema, HState#state{conn =Conn} , Handler),
			work_loop(State);
		{'EXIT', _From, _Reason} ->
			ok;
		_ ->
			work_loop(State)
  end.


connection(#modstate{host = Host, port = Port, auto_reconnect = AutoReconnect} = State)  ->
  case riakc_pb_socket:start_link(Host, Port, [{auto_reconnect, AutoReconnect}]) of 
	{ok, Pid} ->
	  {ok, State#modstate{conn = Pid}};
	{error, Reason} ->
	  lager:error("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
					  [Host, Port, Reason]),
	  {error, Reason}
  end.

handle_create_schema(Schema, HState, Handler) ->
	case Handler:create_schema(Schema, HState) of
		{ok, NewState_} -> {ok, NewState_};
		{error, Error, NewState_} -> {{error, Error}, NewState_}
	end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server stuff.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_riak_conn(PoolName) ->
  case ets:lookup(sumo_pool, PoolName) of 
  [] ->
	wpool:call(PoolName, get_connection);
  Pids ->
	{_, Conn} = lists:nth(erlang:phash(erlang:timestamp(), length(Pids)), Pids),
	Conn
  end. 

-spec riak_opts([term()]) -> [term()].
riak_opts(Options) ->
  User = proplists:get_value(username, Options),
  Pass = proplists:get_value(password, Options),
  Opts0 = case User /= undefined andalso Pass /= undefined of
			true -> [{credentials, User, Pass}];
			_    -> []
		  end,
  Opts1 = case lists:keyfind(connect_timeout, 1, Options) of
			{_, V1} -> [{connect_timeout, V1}, {auto_reconnect, true}] ++ Opts0;
			_       -> [{auto_reconnect, true}] ++ Opts0
		  end,
  Opts1.


get_conn_info(write) ->
  wpool:call(?WRITE, get_conn_info);

get_conn_info(read) -> 
  wpool:call(?READ, get_conn_info);

get_conn_info(_) ->
  ok.


statistic(write) ->
  Get = fun proplists:get_value/2,
  InitStats = ?THROW_TO_ERROR(wpool:stats(?WRITE)),
  PoolPid = Get(supervisor, InitStats),
  Options = Get(options, InitStats),
  InitWorkers = Get(workers, InitStats),
  WorkerStatus = 
  [begin
	  WorkerStats = Get(I, InitWorkers),
	  MsgQueueLen = Get(message_queue_len, WorkerStats),
	  Memory = Get(memory, WorkerStats),
	  {status, WorkerStats, MsgQueueLen, Memory}
	end || I <- lists:seq(1, length(InitWorkers))],
	[PoolPid, Options, WorkerStatus];


statistic(read) ->
  Get = fun proplists:get_value/2,
  InitStats = ?THROW_TO_ERROR(wpool:stats(?READ)),
  PoolPid = Get(supervisor, InitStats),
  Options = Get(options, InitStats),
  InitWorkers = Get(workers, InitStats),
  WorkerStatus = 
  [begin
	  WorkerStats = Get(I, InitWorkers),
	  MsgQueueLen = Get(message_queue_len, WorkerStats),
	  Memory = Get(memory, WorkerStats),
	  {status, WorkerStats, MsgQueueLen, Memory}
	end || I <- lists:seq(1, length(InitWorkers))],
	[PoolPid, Options, WorkerStatus];

statistic(_) ->
  ok.

pool_name(write) -> ?WRITE;
pool_name(read) -> ?READ;
pool_name(_) -> ok.

