%%% @doc MySql repository implementation.
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
-module(sumo_repo_mysql).
-author("Marcelo Gornstein <marcelog@gmail.com>").
-github("https://github.com/inaka").
-license("Apache License 2.0").

-include_lib("emysql/include/emysql.hrl").

-behavior(sumo_repo).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Public API.
-export([init/1]).
-export([create_schema/2]).
-export([persist/2]).
-export([delete/3, delete_by/3, delete_all/2]).
-export([prepare/3, execute/2, execute/3]).
-export([find_all/2, find_all/5, find_by/3, find_by/5]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {pool :: atom() | pid()}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Options) ->
  % The storage backend key in the options specifies the name of the process
  % which creates and initializes the storage backend.
  Backend = proplists:get_value(storage_backend, Options),
  Pool    = sumo_backend_mysql:get_pool(Backend),
  {ok, #state{pool=Pool}}.

persist(Doc, State) ->
  % Set the real id, replacing undefined by 0 so it is autogenerated
  DocName = sumo_internal:doc_name(Doc),
  IdField = sumo_internal:id_field_name(DocName),
  NewId   =
    case sumo_internal:get_field(IdField, Doc) of
      undefined -> 0;
      Id        -> Id
    end,
  NewDoc = sumo_internal:set_field(IdField, NewId, Doc),
  % Needed because the queries will carry different number of arguments.
  Statement =
    case NewId of
      0     -> insert;
      NewId -> update
    end,

  Fields   = sumo_internal:doc_fields(NewDoc),
  NPFields = Fields -- [{IdField, NewId}], % Non-primary fields.

  Fun =
    fun() ->
      [ColumnDqls, ColumnSqls] =
        lists:foldl(
          fun({Name, _Value}, [Dqls, Sqls]) ->
            Dql = [escape(atom_to_list(Name))],
            Sql = "?",
            [[Dql | Dqls], [Sql | Sqls]]
          end,
          [[], []],
          Fields
        ),

      NPColumnDqls =
        lists:foldl(
          fun({Name, _Value}, Dqls) ->
            Dql = [escape(atom_to_list(Name))],
            [Dql | Dqls]
          end,
          [],
          NPFields
        ),

      TableName          = escape(atom_to_list(DocName)),
      ColumnsText        = string:join(ColumnDqls, ","),
      InsertValueSlots   = string:join(ColumnSqls, ","),
      OnDuplicateColumns = [[ColumnName, "=?"] || ColumnName <- NPColumnDqls],
      OnDuplicateSlots   = string:join(OnDuplicateColumns, ","),

      [ "INSERT INTO ", TableName
      , " (", ColumnsText, ")"
      , " VALUES (", InsertValueSlots, ")"
      , " ON DUPLICATE KEY UPDATE "
      , OnDuplicateSlots
      ]
    end,

  StatementName   = prepare(DocName, Statement, Fun),
  ColumnValues    = lists:reverse([V || {_K, V} <- Fields]),
  NPColumnValues  = lists:reverse([V || {_K, V} <- NPFields]),
  StatementValues = lists:append(ColumnValues, NPColumnValues),

  case execute(StatementName, StatementValues, State) of
    #ok_packet{insert_id = InsertId} ->
      % XXX TODO darle una vuelta mas de rosca
      % para el manejo general de cuando te devuelve el primary key
      % considerar el caso cuando la primary key (campo id) no es integer
      % tenes que poner unique index en lugar de primary key
      % la mejor solucion es que el PK siempre sea un integer, como hace mongo
      LastId =
        case InsertId of
          0 -> NewId;
          I -> I
        end,
      IdField = sumo_internal:id_field_name(DocName),
      {ok, sumo_internal:set_field(IdField, LastId, Doc), State};
    Error ->
      evaluate_execute_result(Error, State)
  end.

delete(DocName, Id, State) ->
  StatementName = prepare(DocName, delete, fun() -> [
    "DELETE FROM ", escape(atom_to_list(DocName)),
    " WHERE ", escape(atom_to_list(sumo_internal:id_field_name(DocName))),
    "=? LIMIT 1"
  ] end),
  case execute(StatementName, [Id], State) of
    #ok_packet{affected_rows = NumRows} -> {ok, NumRows > 0, State};
    Error -> evaluate_execute_result(Error, State)
  end.

delete_by(DocName, Conditions, State) ->
  {Values, CleanConditions} = values_conditions(Conditions),
  Clauses = build_where_clause(CleanConditions),
  HashClause = hash(Clauses),
  PreStatementName = list_to_atom("delete_by_" ++ HashClause),

  StatementFun =
    fun() ->
      [ "DELETE FROM ",
        escape(atom_to_list(DocName)),
        " WHERE ",
        lists:flatten(Clauses)
      ]
    end,
  StatementName = prepare(DocName, PreStatementName, StatementFun),
  Values = [V || {_K, V} <- Conditions],
  case execute(StatementName, Values, State) of
    #ok_packet{affected_rows = NumRows} -> {ok, NumRows, State};
    Error -> evaluate_execute_result(Error, State)
  end.

delete_all(DocName, State) ->
  StatementName = prepare(DocName, delete_all, fun() ->
    ["DELETE FROM ", escape(atom_to_list(DocName))]
  end),
  case execute(StatementName, State) of
    #ok_packet{affected_rows = NumRows} -> {ok, NumRows, State};
    Error -> evaluate_execute_result(Error, State)
  end.

find_all(DocName, State) ->
  find_all(DocName, undefined, 0, 0, State).

find_all(DocName, OrderField, Limit, Offset, State) ->
  % Select * is not good...
  Sql0 = ["SELECT * FROM ", escape(atom_to_list(DocName)), " "],
  {Sql1, ExecArgs1} =
    case OrderField of
      undefined -> {Sql0, []};
      _         -> {[Sql0, " ORDER BY ? "], [atom_to_list(OrderField)]}
    end,
  {Sql2, ExecArgs2} =
    case Limit of
      0     -> {Sql1, ExecArgs1};
      Limit -> {[Sql1, " LIMIT ?,?"], lists:append(ExecArgs1, [Offset, Limit])}
    end,

  StatementName = prepare(DocName, find_all, fun() -> Sql2 end),

  case execute(StatementName, ExecArgs2, State) of
    #result_packet{rows = Rows, field_list = Fields} ->
      Docs   = lists:foldl(
        fun(Row, DocList) ->
          NewDoc = lists:foldl(
            fun(Field, [Doc,N]) ->
              FieldRecord = lists:nth(N, Fields),
              FieldName = list_to_atom(binary_to_list(FieldRecord#field.name)),
              [sumo_internal:set_field(FieldName, Field, Doc), N+1]
            end,
            [sumo_internal:new_doc(DocName, []), 1],
            Row
          ),
          [hd(NewDoc)|DocList]
        end,
        [],
        Rows
      ),
      {ok, lists:reverse(Docs), State};
    Error -> evaluate_execute_result(Error, State)
  end.

%% XXX We should have a DSL here, to allow querying in a known language
%% to be translated by each driver into its own.
find_by(DocName, Conditions, Limit, Offset, State) ->
  {Values, CleanConditions} = values_conditions(Conditions),
  Clauses = build_where_clause(CleanConditions),
  PreStatementName0 = hash(Clauses),

  PreStatementName1 =
    case Limit of
      0     -> PreStatementName0;
      Limit -> PreStatementName0 ++ "_limit"
    end,

  PreName = list_to_atom("find_by_" ++ PreStatementName1),

  Fun = fun() ->
    % Select * is not good..
    Sql1 = [ "SELECT * FROM ",
             escape(atom_to_list(DocName)),
             " WHERE ",
             lists:flatten(Clauses)
           ],

    Sql2 = case Limit of
      0 -> Sql1;
      _ -> [Sql1|[" LIMIT ?, ?"]]
    end,
    Sql2
  end,

  StatementName = prepare(DocName, PreName, Fun),

  ExecArgs =
    case Limit of
      0     -> Values;
      Limit -> lists:flatten([Values | [Offset, Limit]])
    end,

  case execute(StatementName, ExecArgs, State) of
    #result_packet{rows = Rows, field_list = Fields} ->
      Docs = lists:foldl(
        fun(Row, DocList) ->
          NewDoc = lists:foldl(
            fun(Field, [Doc,N]) ->
              FieldRecord = lists:nth(N, Fields),
              FieldName = list_to_atom(binary_to_list(FieldRecord#field.name)),
              [sumo_internal:set_field(FieldName, Field, Doc), N+1]
            end,
            [sumo_internal:new_doc(DocName, []), 1],
            Row
          ),
          [hd(NewDoc)|DocList]
        end,
        [],
        Rows
      ),
      {ok, lists:reverse(Docs), State};
    Error -> evaluate_execute_result(Error, State)
  end.

find_by(DocName, Conditions, State) ->
  find_by(DocName, Conditions, 0, 0, State).

%% XXX: Refactor:
%% Requires {length, X} to be the first field attribute in order to form the
%% correct query. :P
%% If no indexes are defined, will put an extra comma :P
%% Maybe it would be better to just use ALTER statements instead of trying to
%% create the schema on the 1st pass. Also, ALTER statements might be better
%% for when we have migrations.
create_schema(Schema, State) ->
  Name = sumo_internal:schema_name(Schema),
  Fields = sumo_internal:schema_fields(Schema),
  FieldsDql = lists:map(fun create_column/1, Fields),
  Indexes = lists:filter(
    fun(T) -> length(T) > 0 end,
    lists:map(fun create_index/1, Fields)
  ),
  Dql = [
    "CREATE TABLE IF NOT EXISTS ", escape(atom_to_list(Name)), " (",
    string:join(FieldsDql, ","), ",", string:join(Indexes, ","),
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"
  ],
  case execute(Dql, State) of
    #ok_packet{} -> {ok, State};
    Error -> evaluate_execute_result(Error, State)
  end.

create_column(Field) ->
  create_column(
    sumo_internal:field_name(Field),
    sumo_internal:field_type(Field),
    sumo_internal:field_attrs(Field)).

create_column(Name, integer, Attrs) ->
  [escape(atom_to_list(Name)), " INT(11) ", create_column_options(Attrs)];

create_column(Name, float, Attrs) ->
  [escape(atom_to_list(Name)), " FLOAT ", create_column_options(Attrs)];

create_column(Name, text, Attrs) ->
  [escape(atom_to_list(Name)), " TEXT ", create_column_options(Attrs)];

create_column(Name, binary, Attrs) ->
  [escape(atom_to_list(Name)), " BLOB ", create_column_options(Attrs)];

create_column(Name, string, Attrs) ->
  [escape(atom_to_list(Name)), " VARCHAR ", create_column_options(Attrs)];

create_column(Name, date, Attrs) ->
  [escape(atom_to_list(Name)), " DATE ", create_column_options(Attrs)];

create_column(Name, datetime, Attrs) ->
  [escape(atom_to_list(Name)), " DATETIME ", create_column_options(Attrs)].

create_column_options(Attrs) ->
  lists:filter(fun(T) -> is_list(T) end, lists:map(
    fun(Option) ->
      create_column_option(Option)
    end,
    Attrs
  )).

create_column_option(auto_increment) ->
  ["AUTO_INCREMENT "];

create_column_option(not_null) ->
  [" NOT NULL "];

create_column_option({length, X}) ->
  ["(", integer_to_list(X), ") "];

create_column_option(_Option) ->
  none.

create_index(Field) ->
  Name = sumo_internal:field_name(Field),
  Attrs = sumo_internal:field_attrs(Field),
  lists:filter(fun(T) -> is_list(T) end, lists:map(
    fun(Attr) ->
      create_index(Name, Attr)
    end,
    Attrs
  )).

create_index(Name, id) ->
  ["PRIMARY KEY(", escape(atom_to_list(Name)), ")"];

create_index(Name, unique) ->
  List = atom_to_list(Name),
  ["UNIQUE KEY ", escape(List), " (", escape(List), ")"];

create_index(Name, index) ->
  List = atom_to_list(Name),
  ["KEY ", escape(List), " (", escape(List), ")"];

create_index(_, _) ->
  none.

prepare(DocName, PreName, Fun) when is_atom(PreName), is_function(Fun) ->
  Name = statement_name(DocName, PreName),
  case emysql_statements:fetch(Name) of
    undefined ->
      Query = iolist_to_binary(Fun()),
      log("Preparing query: ~p: ~p", [Name, Query]),
      ok = emysql:prepare(Name, Query);
    Q ->
      log("Using already prepared query: ~p: ~p", [Name, Q])
  end,
  Name.

%% @doc Call prepare/3 first, to get a well formed statement name.
execute(Name, Args, #state{pool=Pool}) when is_atom(Name), is_list(Args) ->
  {Time, Value} = timer:tc( emysql, execute, [Pool, Name, Args] ),
  log("Executed Query: ~s -> ~p (~pms)", [Name, Args, Time/1000]),
  Value.

execute(Name, State) when is_atom(Name) ->
  execute(Name, [], State);

execute(PreQuery, #state{pool=Pool}) when is_list(PreQuery)->
  Query = iolist_to_binary(PreQuery),
  {Time, Value} = timer:tc( emysql, execute, [Pool, Query] ),
  log("Executed Query: ~s (~pms)", [Query, Time/1000]),
  Value.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc We can extend this to wrap around emysql records, so they don't end up
%% leaking details in all the repo.
evaluate_execute_result(#error_packet{status = Status, msg = Msg}, State) ->
  {error, <<Status/binary, ":", (list_to_binary(Msg))/binary>>, State}.

escape(Name) when is_atom(Name) ->
  ["`", atom_to_list(Name), "`"];
escape(String) ->
  ["`", String, "`"].

statement_name(DocName, StatementName) ->
  list_to_atom(string:join(
    [atom_to_list(DocName), atom_to_list(StatementName), "stmt"], "_"
  )).

log(Msg, Args) ->
  case application:get_env(sumo_db, log_queries) of
    {ok, true} -> lager:debug(Msg, Args);
    _          -> ok
  end.

-spec values_conditions(sumo_internal:expression()) ->
  {[any()], sumo_internal:expression()}.
values_conditions([Expr | RestExprs]) ->
  {Values, CleanExpr} = values_conditions(Expr),
  {ValuesRest, CleanRestExprs} = values_conditions(RestExprs),
  {Values ++ ValuesRest, [CleanExpr | CleanRestExprs]};
values_conditions({LogicalOp, Exprs})
  when (LogicalOp == 'and')
       or (LogicalOp == 'or')
       or (LogicalOp == 'not') ->
  {Values, CleanExprs} = values_conditions(Exprs),
  {Values, {LogicalOp, CleanExprs}};
values_conditions({Name, Op, Value}) when not is_atom(Value) ->
  {[Value], {Name, Op, '?'}};
values_conditions({Name, Value})
  when Value =/= 'null', Value =/= 'not_null' ->
  {[Value], {Name, '?'}};
values_conditions(Terminal) ->
  {[], Terminal}.

-spec build_where_clause(sumo_internal:expression()) -> iodata().
build_where_clause(Exprs) when is_list(Exprs) ->
  Clauses = lists:map(fun build_where_clause/1, Exprs),
  ["(", interpose(" AND ", Clauses), ")"];
build_where_clause({'and', Exprs}) ->
  build_where_clause(Exprs);
build_where_clause({'or', Exprs}) ->
  Clauses = lists:map(fun build_where_clause/1, Exprs),
  ["(", interpose(" OR ", Clauses), ")"];
build_where_clause({'not', Expr}) ->
  [" NOT ", "(", build_where_clause(Expr), ")"];
build_where_clause({Name, Op, '?'}) ->
  [escape(Name), " ", atom_to_list(Op), " ? "];
build_where_clause({Name1, Op, Name2}) ->
  [escape(Name1), " ", atom_to_list(Op), " ", escape(Name2)];
build_where_clause({Name, '?'}) ->
  [escape(Name), " = ? "];
build_where_clause({Name, 'null'}) ->
  [escape(Name), " IS NULL "];
build_where_clause({Name, 'not_null'}) ->
  [escape(Name), " IS NOT NULL "].

interpose(Sep, List) ->
  interpose(Sep, List, []).

interpose(_Sep, [], Result) ->
  lists:reverse(Result);
interpose(Sep, [Item | []], Result) ->
  interpose(Sep, [], [Item | Result]);
interpose(Sep, [Item | Rest], Result) ->
  interpose(Sep, Rest, [Sep, Item | Result]).

-spec hash(iodata()) -> string().
hash(Clause) ->
  Bin = crypto:hash(md5, Clause),
  List = binary_to_list(Bin),
  Fun = fun(Num) -> string:right(integer_to_list(Num, 16), 2, $0) end,
  lists:flatmap(Fun, List).
