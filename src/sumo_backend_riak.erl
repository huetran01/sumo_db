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

-include_lib("riakc/include/riakc.hrl").
-include("sumo.hrl").

-define(THROW_TO_ERROR(X), try X catch throw:Result -> erlang:raise(error, Result, erlang:get_stacktrace()) end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(modstate, {host :: string(),
                port :: non_neg_integer(),
                opts :: [term()]}).

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
init([undefined, #modstate{host = Host, port = Port, opts = Opts}]) ->
  {ok, Conn} = riakc_pb_socket:start_link(Host, Port, Opts),
  {ok, Conn};

init(Options) ->
  %% Get connection parameters
  Host = proplists:get_value(host, Options, "127.0.0.1"),
  Port = proplists:get_value(port, Options, 8087),
  PoolSize = proplists:get_value(poolsize, Options, 50),
  WritePoolSize = proplists:get_value(write_pool_size, Options, PoolSize),
  ReadPoolSize = proplists:get_value(read_pool_size, Options, 50),
  Opts = riak_opts(Options),
  WritePoolOptions    = [ {overrun_warning, 10000}
                    , {overrun_handler, {sumo_internal, report_overrun}}
                    , {workers, WritePoolSize}
                    , {worker, {?MODULE, [undefined, #modstate{host = Host, port = Port, opts = Opts}]}}],
  ReadPoolOptions    = [ {overrun_warning, 10000}
                    , {overrun_handler, {sumo_internal, report_overrun}}
                    , {workers, ReadPoolSize}
                    , {worker, {?MODULE, [undefined, #modstate{host = Host, port = Port, opts = Opts}]}}],
  wpool:start_pool(?WRITE, WritePoolOptions),
  wpool:start_pool(?READ, ReadPoolOptions),
  {ok, #modstate{host = Host, port = Port, opts = Opts}}.

%% @todo: implement connection pool.
%% In other cases is a built-in feature of the client.
-spec handle_call(term(), term(), state()) -> {reply, term(), state()}.
handle_call(get_connection,
            _From,
            State = #modstate{host = Host, port = Port, opts = Opts}) ->
  {ok, Conn} = riakc_pb_socket:start_link(Host, Port, Opts),
  {reply, Conn, State};

handle_call(get_conn_info, _From, Conn = State) ->
  {reply, Conn, State};

handle_call({create_schema, Schema, HState, Handler}, _From, Conn = State) ->
  {Result, _} = case Handler:create_schema(Schema, HState#state{conn = Conn}) of
  {ok, NewState_} -> {ok, NewState_};
  {error, Error, NewState_} -> {{error, Error}, NewState_}
  end,
  {reply, Result, State};


handle_call({persist, Doc, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:persist(Doc, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({persist, OldObj, Doc, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:persist(OldObj, Doc, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({delete_by, DocName, Conditions, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:delete_by(DocName, Conditions, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};


handle_call({delete_all, DocName, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:delete_all(DocName, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};


handle_call({find_all, DocName, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_all(DocName, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({find_all, DocName, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_all(DocName, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({find_by, DocName, Conditions, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({find_by, DocName, Conditions, Limit, Offset, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Limit, Offset, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({find_by, DocName, Conditions, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({find_by, DocName, Conditions, Filter, SortFields, Limit, Offset, HState, Handler}, _From, Conn = State) ->
  {OkOrError, Reply, _} = Handler:find_by(DocName, Conditions, Filter, SortFields, Limit, Offset, HState#state{conn = Conn}),
  {reply, {OkOrError, Reply}, State};

handle_call({call, Handler, Function, Args, DocName, HState}, _From, Conn = State) ->
  RealArgs = lists:append(Args, [DocName, HState#state{conn = Conn}]),
  {OkOrError, Reply, _} = erlang:apply(Handler, Function, RealArgs),
  {reply, {OkOrError, Reply}, State};

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Unused Callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({persist, Doc, HState, Handler}, Conn = State) ->
  {_OkOrError, Reply, _} = Handler:persist(Doc, HState#state{conn = Conn}),
  %% TODO next: should call event here
  lager:debug("sumo: asyn_persist: Doc: ~p,  Reply: ~p ~n",[Doc, Reply]),
  {noreply, State};

handle_cast({persist, OldObj, Doc, HState, Handler}, Conn = State) ->
  {_OkOrError, Reply, _} = Handler:persist(OldObj, Doc, HState#state{conn = Conn}),
  lager:debug("sumo: asyn_persist: Doc: ~p, Reply: ~p",[Doc, Reply]),
  {noreply, State};


handle_cast({delete_by, DocName, Conditions, HState, Handler}, Conn = State) ->
  {_OkOrError, Reply, _} = Handler:delete_by(DocName, Conditions, HState#state{conn = Conn}),
  %% TODO: next : should call event here 
  lager:debug("sumo: asyn_delete_by: Doc: ~p, Reply: ~p ~n",[DocName, Reply]),
  {noreply, State};

handle_cast({delete_all, DocName, HState, Handler}, Conn = State) ->
  {_OkOrError, Reply, _} = Handler:delete_all(DocName, HState#state{conn = Conn}),
  %% TODO: next: should call event here
  lager:debug("sumo: async_delete_all: Doc: ~p, Reply: ~p ~n",[DocName, Reply]),
  {noreply, State};

handle_cast(_Msg, State) -> {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server stuff.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

