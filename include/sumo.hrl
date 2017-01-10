

-define(WRITE, sumo_write_pool).
-define(READ, sumo_read_pool).
-define(STRATEGY, wpool:default_strategy()).

-define(INVALID_QUERY, <<"(())">>).
-define(IGNORE_KEY, [<<"_yz_id">>, <<"_yz_rb">>, <<"_yz_rk">>, <<"_yz_rt">>, <<"score">>]).
-define(LIMIT, 10000).

-type connection() :: pid().
-type index()      :: binary().
-type options()    :: [proplists:property()].

