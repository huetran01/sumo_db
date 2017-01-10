-module(sumo_store_opts).


%% API
-export([
	init/1, get_key/1, get_all/0
]).


-define(TEMP_MODULE, sumo_store_opts_data).

-spec init(list()) -> ok.
init(Opts) ->
	term_compiler:compile(?TEMP_MODULE, [
		{ get, { list, Opts }},
		{ get_all, {term, Opts}}
	]).

-spec get_key(atom()) -> term().
get_key(Key) ->
	?TEMP_MODULE:get(Key).

-spec get_all() -> list().
get_all() ->
	?TEMP_MODULE:get_all().



