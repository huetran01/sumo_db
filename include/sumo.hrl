

-define(WRITE, sumo_write_pool).
-define(READ, sumo_read_pool).
-define(STRATEGY, wpool:default_strategy()).

-type connection() :: pid().
-type index()      :: binary().
-type options()    :: [proplists:property()].