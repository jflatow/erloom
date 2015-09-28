-module(erloom_chain).

%% erloom api
-export([lookup/2,
         modify/3,
         accrue/3,
         accrue/4]).

%% public api
-export([lock/3,
         unlock/3,
         value/2,
         version/2]).

lookup(State, Path) ->
    util:lookup(State, Path, {undefined, undefined}).

modify(State, Path, {Fun, Version}) when is_function(Fun) ->
    util:modify(State, Path, {Fun(value(State, Path)), Version});
modify(State, Path, {Fun, Version, Lock}) when is_function(Fun) ->
    util:modify(State, Path, {Fun(value(State, Path)), Version, Lock});
modify(State, Path, {_, _} = Term) ->
    util:modify(State, Path, Term);
modify(State, Path, {_, _, _} = Term) ->
    util:modify(State, Path, Term).

accrue(State, Path, {Value, Version}) ->
    accrue(State, Path, {Value, Version}, fun util:op/2).

accrue(State, Path, {Value, Version}, Op) ->
    modify(State, Path, {fun (Prior) -> Op(Prior, Value) end, Version}).

lock(State, Path, Lock) ->
    case lookup(State, Path) of
        {Value, Version} ->
            util:modify(State, Path, {Value, Version, Lock});
        {_, _, Lock} ->
            State
    end.

unlock(State, Path, Lock) ->
    case lookup(State, Path) of
        {_, _} ->
            State;
        {Value, Version, Lock} ->
            util:modify(State, Path, {Value, Version});
        {_, _, _} ->
            State
    end.

value(State, Path) ->
    case lookup(State, Path) of
        {Value, _} ->
            Value;
        {Value, _, _} ->
            Value
    end.

version(State, Path) ->
    case lookup(State, Path) of
        {_, Version} ->
            Version;
        {_, Version, _} ->
            Version
    end.
