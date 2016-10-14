

-define(WRITE, sumo_write_pool).
-define(READ, sumo_read_pool).

-type connection() :: pid().
-type index()      :: binary().
-type options()    :: [proplists:property()].