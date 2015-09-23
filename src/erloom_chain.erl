-module(erloom_chain).

%% erloom api
-export([lookup/2,
         modify/3]).

%% public api
-export([lock/2,
         unlock/3,
         value/2,
         version/2]).

lookup(State, Path) ->
    util:lookup(State, Path, {undefined, undefined}).

modify(State, Path, {Fun, Version}) when is_function(Fun) ->
    util:modify(State, Path, {Fun(value(State, Path)), Version});
modify(State, Path, {Fun, Version, locked}) when is_function(Fun) ->
    util:modify(State, Path, {Fun(value(State, Path)), Version, locked});
modify(State, Path, {_, _} = Term) ->
    util:modify(State, Path, Term);
modify(State, Path, {_, _, locked} = Term) ->
    util:modify(State, Path, Term).

lock(State, Path) ->
    case lookup(State, Path) of
        {Value, Version} ->
            util:modify(State, Path, {Value, Version, locked});
        {_, _, locked} ->
            State
    end.

unlock(State, Path, Version) ->
    case lookup(State, Path) of
        {_, _} ->
            State;
        {Value, Version, locked} ->
            util:modify(State, Path, {Value, Version})
    end.

value(State, Path) ->
    case lookup(State, Path) of
        {Value, _} ->
            Value;
        {Value, _, locked} ->
            Value
    end.

version(State, Path) ->
    case lookup(State, Path) of
        {_, Version} ->
            Version;
        {_, Version, locked} ->
            Version
    end.
