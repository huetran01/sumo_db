-module(sumo_util).

-export([to_bin/1
		,to_atom/1
		,to_str/1
		,build_sort_query/1
		,build_query/1
    ,suffix/1]).


%% @private
-spec to_bin(term()) -> binary().
to_bin(Data) when is_integer(Data) ->
  integer_to_binary(Data);
to_bin(Data) when is_float(Data) ->
  float_to_binary(Data, [{decimals, 4}, compact]);
to_bin(Data) when is_atom(Data) ->
  atom_to_binary(Data, utf8);
to_bin(Data) when is_list(Data) ->
  iolist_to_binary(Data);
to_bin(Data) ->
  Data.

%% @private
-spec to_atom(term()) -> binary().
to_atom(Data) when is_binary(Data) ->
  binary_to_atom(Data, utf8);
to_atom(Data) when is_list(Data) ->
  list_to_atom(Data);
to_atom(Data) when is_pid(Data); is_reference(Data); is_tuple(Data) ->
  list_to_atom(integer_to_list(erlang:phash2(Data)));
to_atom(Data) ->
  Data.

-spec to_str(term()) -> binary().
to_str(Data) when is_binary(Data) ->
  binary_to_list(Data) ;
to_str(Data) when is_integer(Data) ->
  integer_to_list(Data) ;
to_str(Data) when is_float(Data) ->
  float_to_list(Data, [{decimals, 4}, compact]) ;
to_str(Data) when is_atom(Data) ->
  atom_to_list(Data) ;
to_str(Data) ->
  Data.


build_sort_key([K], <<"">>) ->
  case suffix(K) of 
  true -> 
   binary:list_to_bin([K, "_set"]);
  _ -> 
    case lists:prefix("geodist(",binary_to_list(K)) of
      false -> 
        binary:list_to_bin([K, "_register"]);  
      _ ->
        binary:list_to_bin([K])
    end
  
  end; 
build_sort_key([K], Acc) ->
  case suffix(K) of 
  true -> 
  	binary:list_to_bin([Acc, ".", K, "_set"]);
  _ -> 
  	binary:list_to_bin([Acc, ".", K, "_register"])
  end;

build_sort_key([K | T], <<"">>) ->
  build_sort_key(T, binary:list_to_bin([K, "_map"]));
build_sort_key([K | T], Acc) ->
  build_sort_key(T, binary:list_to_bin([Acc, ".", K, "_map"])).

-spec build_sort_query([] | [tuple()]) -> binary().
build_sort_query(SortQuery) -> 
  SortList = build_sort_query(SortQuery, []),
  to_bin(string:join(lists:reverse(SortList), ",")).

build_sort_query([], Res) -> Res ;
build_sort_query([{Key, Sort} | T], Res)  ->
  SortKey = build_sort_key([to_bin(Key)], <<>>),
  SortVal = to_bin(Sort),
  BinVal = binary_to_list(<<SortKey/binary, " ", SortVal/binary>>),  
  build_sort_query(T, [BinVal|Res]).

-spec build_query([] | [tuple() | tuple()]) -> binary().
build_query(Conditions) -> 
 build_query1(Conditions, fun escape/1, fun quote/1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Private API - Query Builder.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
build_query1([], _EscapeFun, _QuoteFun) ->
  <<"*:*">>;
build_query1(Exprs, EscapeFun, QuoteFun) when is_list(Exprs) ->
  Clauses = [build_query1(Expr, EscapeFun, QuoteFun) || Expr <- Exprs],
  binary:list_to_bin(["(", interpose(" AND ", Clauses), ")"]);
build_query1({'and', Exprs}, EscapeFun, QuoteFun) ->
  build_query1(Exprs, EscapeFun, QuoteFun);
build_query1({'or', Exprs}, EscapeFun, QuoteFun) ->
  Clauses = [build_query1(Expr, EscapeFun, QuoteFun) || Expr <- Exprs],
  binary:list_to_bin(["(", interpose(" OR ", Clauses), ")"]);
build_query1({'not', Expr}, EscapeFun, QuoteFun) ->
  binary:list_to_bin(["(NOT ", build_query1(Expr, EscapeFun, QuoteFun), ")"]);
build_query1({Name, '<', Value}, EscapeFun, _QuoteFun) ->
  NewVal = binary:list_to_bin(["{* TO ", EscapeFun(Value), "}"]),
  query_eq(Name, NewVal);
build_query1({Name, '=<', Value}, EscapeFun, _QuoteFun) ->
  NewVal = binary:list_to_bin(["[* TO ", EscapeFun(Value), "]"]),
  query_eq(Name, NewVal);
build_query1({Name, '>', Value}, EscapeFun, _QuoteFun) ->
  NewVal = binary:list_to_bin(["{", EscapeFun(Value), " TO *}"]),
  query_eq(Name, NewVal);
build_query1({Name, '>=', Value}, EscapeFun, _QuoteFun) ->
  NewVal = binary:list_to_bin(["[", EscapeFun(Value), " TO *]"]),
  query_eq(Name, NewVal);
build_query1({Name, '==', Value}, EscapeFun, QuoteFun) ->
  build_query1({Name, Value}, EscapeFun, QuoteFun);
build_query1({Name, '/=', Value}, EscapeFun, QuoteFun) ->
  build_query1({negative_field(Name), Value}, EscapeFun, QuoteFun);
build_query1({Name, 'like', Value}, _EscapeFun, _QuoteFun) ->
  NewVal = like_to_wildcard_search(Value),
  Bypass = fun(X) -> X end,
  build_query1({Name, NewVal}, Bypass, Bypass);
build_query1({Name, 'null'}, _EscapeFun, _QuoteFun) ->
  %% null: (Field:undefined OR (NOT Field:[* TO *]))
  Val = {'or', [{Name, <<"undefined">>}, {'not', {Name, <<"[* TO *]">>}}]},
  Bypass = fun(X) -> X end,
  build_query1(Val, Bypass, Bypass);
build_query1({Name, 'not_null'}, _EscapeFun, _QuoteFun) ->
  %% not_null: (Field:[* TO *] AND -Field:undefined)
  Val = {'and', [{Name, <<"[* TO *]">>}, {Name, '/=', <<"undefined">>}]},
  Bypass = fun(X) -> X end,
  build_query1(Val, Bypass, Bypass);

build_query1({_Name, 'in', []}, EscapeFun, QuoteFun) ->
  build_query1({'or', []}, EscapeFun, QuoteFun);

build_query1({Name, 'in', ValList}, _EscapeFun, _QuoteFun)  ->
  NewValList = 
  lists:map(fun(Var) ->
    "\""  ++ to_str(Var) ++ "\"" 
  end, ValList),
  ValStr = string:join(NewValList, " "),
  NewVal = binary:list_to_bin(["(", ValStr, ")"]),
  query_eq(Name, NewVal);

build_query1({Name, Value}, _EscapeFun, QuoteFun) ->
  query_eq(Name, QuoteFun(Value)).

%% @private
query_eq(K, V) ->
  binary:list_to_bin([build_key(K), V]).

%% @private
build_key(K) ->
  build_key(binary:split(to_bin(K), <<".">>, [global]), <<"">>).

%% @private
build_key([K], <<"">>) ->
  case suffix(K) of 
  true -> 
  binary:list_to_bin([K, "_set:"]);
  _ ->
  binary:list_to_bin([K, "_register:"])
  end;

build_key([K], Acc) ->
  case suffix(K) of 
  true ->
  binary:list_to_bin([Acc, ".", K, "_set:"]);
  _ ->
  binary:list_to_bin([Acc, ".", K, "_register:"])
  end;

build_key([K | T], <<"">>) ->
  build_key(T, binary:list_to_bin([K, "_map"]));
build_key([K | T], Acc) ->
  build_key(T, binary:list_to_bin([Acc, ".", K, "_map"])).

-spec suffix(term()) -> boolean().
suffix(Key) ->
  KeyStr = to_str(Key),
  lists:suffix("arr", KeyStr).

%% @private
interpose(Sep, List) ->
  interpose(Sep, List, []).

%% @private
interpose(_Sep, [], Result) ->
  lists:reverse(Result);
interpose(Sep, [Item | []], Result) ->
  interpose(Sep, [], [Item | Result]);
interpose(Sep, [Item | Rest], Result) ->
  interpose(Sep, Rest, [Sep, Item | Result]).

%% @private
negative_field(Name) ->
  binary:list_to_bin([<<"-">>, to_bin(Name)]).

%% @private
quote(Value) ->
  [$\", re:replace(to_bin(Value), "[\\\"\\\\]", "\\\\&", [global]), $\"].

%% @private
escape(Value) ->
  Escape = "[\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\]",
  re:replace(to_bin(Value), Escape, "\\\\&", [global, {return, binary}]).

%% @private
whitespace(Value) ->
  re:replace(Value, "[\\\s\\\\]", "\\\\&", [global, {return, binary}]).

%% @private
like_to_wildcard_search(Like) ->
  whitespace(binary:replace(to_bin(Like), <<"%">>, <<"*">>, [global])).