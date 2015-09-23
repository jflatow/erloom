-module(erloom_surety).

-export([task/4,
         handle_task/3,
         pause_tasks/1,
         launch_tasks/1,
         restart_tasks/1]).

task(Base = #{name := Name}, Node, Task, State) ->
    %% just insert into the table, it gets run once tasks are launched
    %% the task only gets launched on the specified node, but all nodes are aware of it
    %% this means tasks with the same name are queued globally, which is very useful
    %% messages emitted by the task will be based on the base
    %% generally it should depend at least on whatever triggered the task
    %%  one may also embed a yarn inside the base
    %% NB: if tasks with the same name are triggered by unordered messages
    %%  or if a task is triggered by the same message for different nodes
    %%  the queue on each node may reflect different truths
    util:modify(State, [tasks, Name],
                fun (undefined) ->
                        {undefined, [{Node, {Task, Base}}]};
                    ({Pid, Stack}) ->
                        {Pid, Stack ++ [{Node, {Task, Base}}]}
                end).

handle_task(#{kind := retry, name := Name, value := Arg}, Node, State) ->
    util:replace(State, [tasks, Name],
                 fun ({Pid, Stack}) ->
                         %% update the arg, it will be used if we respawn
                         {Pid, util:replace(Stack, Node,
                                            fun ({{Fun, _}, Base}) ->
                                                    {{Fun, Arg}, Base}
                                            end)}
                 end);
handle_task(#{kind := kill, name := Name} = Message, Node, State) ->
    %% allow outsiders to safely stop the task
    case util:lookup(State, [tasks, Name]) of
        {P, _} when is_pid(P) ->
            exit(P, silent);
        _ ->
            ok
    end,
    complete_task(Message, Node, State);
handle_task(#{kind := done} = Message, Node, State) ->
    complete_task(Message, Node, State);
handle_task(#{kind := fail} = Message, Node, State) ->
    complete_task(Message, Node, State).

complete_task(#{name := Name, value := Result} = Message, Node, State) ->
    State1 =
        case util:lookup(State, [tasks, Name]) of
            {Pid, Stack} ->
                %% remove the first task with name / node
                case lists:keydelete(Node, 1, Stack) of
                    [] ->
                        %% if stack is empty, delete the whole thing
                        util:remove(State, [tasks, Name]);
                    Stack1 when Pid =:= undefined ->
                        %% if we haven't started spawning yet, just remove
                        util:modify(State, [tasks, Name], {Pid, Stack1});
                    Stack1 when is_pid(Pid) ->
                        %% spawn the next task (maybe, if its ours)
                        Pid1 = spawn_task(Name, Stack1, State),
                        util:modify(State, [tasks, Name], {Pid1, Stack1})
                end;
            _ ->
                %% somehow doesn't exist, can't happen normally, but keep going anyway
                State
        end,
    loom:task_completed(Message, Node, Result, State1).

spawn_task(_, [{Node, _}|_], _) when Node =/= node() ->
    undefined;
spawn_task(_, [{_, {{Fun, Arg}, Base}}|_], State = #{listener := L}) ->
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
                        loom:call(L, Base#{type => task, kind => retry, value => A1}),
                        receive after time:timeout(Wait) -> Loop(A1, loom:state(L)) end;
                    {done, Result} ->
                        %% when we are done, notify the loom so we can finish the task
                        loom:call(L, Base#{type => task, kind => done, value => Result});
                    Other ->
                        %% user failure happens, treat it like done but different
                        loom:call(L, Base#{type => task, kind => fail, value => Other})
                end
        end,
    spawn_link(fun () -> Run(Arg, State) end).

pause_tasks(State = #{tasks := Tasks}) ->
    %% silently exit tasks, and mark them all as unlaunched
    Tasks1 =
        util:map(Tasks,
                 fun ({Pid, Stack}) when is_pid(Pid) ->
                         exit(Pid, silent),
                         {undefined, Stack};
                     ({undefined, Stack}) ->
                         {undefined, Stack}
                 end),
    State#{tasks => Tasks1}.

launch_tasks(State = #{tasks := Tasks}) ->
    %% launch any unlaunched tasks (i.e. only after we are working off tip)
    Tasks1 =
        maps:fold(fun (Name, {undefined, Stack}, Acc) ->
                          Acc#{Name => {spawn_task(Name, Stack, State), Stack}};
                      (_, _, Acc) ->
                          Acc
                  end, Tasks, Tasks),
    State#{tasks => Tasks1}.

restart_tasks(State = #{tasks := Tasks}) ->
    %% restart all tasks in the table (i.e. after we wake up)
    %% mark everything as unlaunched, then launch
    %% NB: we don't pause and launch, as the Pids might be stale (though not likely)
    Tasks1 = util:map(Tasks, fun ({_, Stack}) -> {undefined, Stack} end),
    launch_tasks(State#{tasks => Tasks1}).
