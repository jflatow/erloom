-module(erloom_surety).

-export([task/3,
         handle_task/2,
         pause_tasks/1,
         launch_tasks/1,
         restart_tasks/1]).

task(Key, Task, State = #{tasks := Tasks}) ->
    %% create task for key, if and only if not already created
    %% keys should normally be unique to begin with
    case util:get(Tasks, Key) of
        undefined ->
            %% just insert into the table, it gets run once tasks are launched
            State#{tasks => Tasks#{Key => {undefined, Task}}};
        _ ->
            %% existing tasks always take precedence
            State
    end.

handle_task(#{key := Key, retry := Arg}, State = #{tasks := Tasks}) ->
    case util:get(Tasks, Key) of
        {P, {F, _}} ->
            %% update the arg, it will be used if we respawn
            State#{tasks => Tasks#{Key => {P, {F, Arg}}}};
        undefined ->
            %% if the task was killed, retry could appear after removal
            State
    end;
handle_task(#{key := Key, kill := _}, State) ->
    %% allow outsiders to safely stop the task
    case util:lookup(State, [tasks, Key]) of
        {P, _} when is_pid(P) ->
            exit(P, normal);
        _ ->
            ok
    end,
    util:remove(State, [tasks, Key]);
handle_task(#{key := Key, done := _}, State) ->
    util:remove(State, [tasks, Key]);
handle_task(#{key := Key, fail := _}, State) ->
    util:remove(State, [tasks, Key]).

spawn_task(Key, {Fun, Arg}, State = #{listener := L}) ->
    Run =
        fun Loop(A, S) ->
                case catch Fun(A, S) of
                    {retry, Wait} ->
                        %% if the arg doesn't change there's no need to send a message
                        receive after time:timeout(Wait) -> Loop(A, loom:state(L)) end;
                    {retry, A1, Wait} ->
                        %% if the arg changes we send a message to notify the loom
                        %% if we crash, we must either:
                        %%  1. try the previous arg again right away
                        %%  2. try the next arg right away
                        %%  3. try the next arg with an additional wait penalty
                        %% regardless, we can't avoid the possibility of repeating an arg
                        %% crashing is rare, all options have issues: do something simple (#2)
                        loom:call(L, #{type => task, key => Key, retry => A1}),
                        receive after time:timeout(Wait) -> Loop(A1, loom:state(L)) end;
                    {done, Result} ->
                        %% when we are done, notify the loom so we can finish the task
                        loom:call(L, #{type => task, key => Key, done => Result});
                    Other ->
                        %% user failure happens, treat it like done but different
                        loom:call(L, #{type => task, key => Key, fail => Other})
                end
        end,
    spawn_link(fun () -> Run(Arg, State) end).

pause_tasks(State = #{tasks := Tasks}) ->
    %% silently exit tasks, and mark them all as unlaunched
    Tasks1 =
        util:map(Tasks,
                 fun ({P, T}) when is_pid(P) ->
                         exit(P, silent),
                         {undefined, T};
                 ({_, T}) ->
                         {undefined, T}
                 end),
    State#{tasks => Tasks1}.

launch_tasks(State = #{tasks := Tasks}) ->
    %% launch any unlaunched tasks (i.e. only after we are working off tip)
    Tasks1 =
        maps:fold(fun (Key, {undefined, Task}, Acc) ->
                          Acc#{Key => {spawn_task(Key, Task, State), Task}};
                      (_, _, Acc) ->
                          Acc
                  end, Tasks, Tasks),
    State#{tasks => Tasks1}.

restart_tasks(State = #{tasks := Tasks}) ->
    %% restart all tasks in the table (i.e. after we wake up)
    %% mark everything as unlaunched, then launch
    %% NB: we don't pause and launch, as the Pids might be stale (though not likely)
    Tasks1 = util:map(Tasks, fun ({_, T}) -> {undefined, T} end),
    launch_tasks(State#{tasks => Tasks1}).
