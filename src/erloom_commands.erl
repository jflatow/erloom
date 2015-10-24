-module(erloom_commands).

-export([handle_command/3]).

handle_command(#{kind := batch, value := Batch}, Node, State) ->
    State1 = lists:foldl(fun (M, S) -> handle_command(M, Node, S) end, State, Batch),
    State1#{response => {ok, {true, undefined}}};
handle_command(Command, Node, State) ->
    Kind = util:get(Command, kind),
    Path = util:get(Command, path),
    case command(Command, State) of
        {{Bool, Former}, S} when Kind =:= chain ->
            S1 = S#{
                   former => Former,
                   response => {ok, {Bool, erloom_chain:value(S, Path)}}
                  },
            loom:command_called(Command, Node, Bool, S1);
        {{Bool, Former}, S} ->
            S1 = S#{
                   former => Former,
                   response => {ok, {Bool, util:lookup(S, Path)}}
                  },
            loom:command_called(Command, Node, Bool, S1);
        S when is_map(S) ->
            loom:command_called(Command, Node, false, S)
    end.

command(#{verb := lookup, path := Path, kind := probe}, State) ->
    case util:lookup(State, [elect, pending, {chain, Path}]) of
        undefined ->
            State#{response => {ok, erloom_chain:value(State, Path)}};
        MotionId ->
            State#{response => {wait, MotionId}}
    end;
command(#{verb := lookup, path := Path, kind := chain}, State) ->
    State#{response => {ok, erloom_chain:value(State, Path)}};
command(#{verb := lookup, path := Path}, State) ->
    State#{response => {ok, util:lookup(State, Path)}};

command(#{verb := modify, path := Path, value := Value, kind := chain} = Command, State) ->
    {{true, erloom_chain:lookup(State, Path)}, erloom_chain:modify(State, Path, {Value, version(Command, State)})};
command(#{verb := modify, path := Path, value := Value}, State) ->
    {{true, util:lookup(State, Path)}, util:modify(State, Path, Value)};

command(#{verb := accrue, path := Path, value := Value, kind := chain} = Command, State) ->
    {{true, erloom_chain:lookup(State, Path)}, erloom_chain:accrue(State, Path, {Value, version(Command, State)})};
command(#{verb := accrue, path := Path, value := Value}, State) ->
    {{true, util:lookup(State, Path)}, util:accrue(State, Path, Value)};

command(#{verb := create, path := Path, value := Value} = Command, State) ->
    erloom_chain:create(State, Path, {Value, version(Command, State)}, #{tagged => true});
command(#{verb := create, path := Path, value := Value, kind := chain}, State) ->
    util:create(State, Path, Value, #{tagged => true});

command(#{verb := remove, path := Path, kind := chain}, State) ->
    erloom_chain:remove(State, Path, #{tagged => true});
command(#{verb := remove, path := Path}, State) ->
    util:remove(State, Path, #{tagged => true});

command(#{verb := swap, path := Path, value := Value, kind := chain} = Command, State) ->
    erloom_chain:swap(State, Path, {Value, version(Command, State)}, #{tagged => true});
command(#{verb := swap, path := Path, value := Value}, State) ->
    util:swap(State, Path, Value, #{tagged => true});

command(_, State) ->
    State.

version(#{version := Version}, _State) ->
    Version;
version(_, State) ->
    loom:after_locus(State).
