-module(erloom_surety).

-export([enqueue_task/4,
         handle_task/3,
         pause_tasks/1,
         launch_tasks/1,
         restart_tasks/1,
         tasks_remaining/2]).

enqueue_task(Base = #{name := Name}, Node, Task, State = #{tasks := Tasks, opts := Opts}) ->
    %% just insert into the table, it gets run once tasks are launched
    %% the task only gets launched on the specified node, but all nodes are aware of it
    %% this means tasks with the same name are queued globally, which is very useful
    %% messages emitted by the task will be based on the base
    %% generally it should depend at least on whatever triggered the task
    %%  one may also embed a yarn inside the base
    %% NB: if tasks with the same name are triggered by unordered messages
    %%  or if a task is triggered by the same message for different nodes
    %%  the queue on each node may reflect different truths
    %% NB: this call may block, in order to apply backpressure when launching tasks
    %%  the limit applies to the number of queues (names) that are allowed concurrently
    %%  the limit can be set to infinity to disable blocking altogether
    #{task_limit := TaskLimit,
      task_wait_for := TaskWaitFor} = Opts,
    Wait =
        case TaskLimit of
            infinity ->
                false;
            N when map_size(Tasks) < N ->
                false;
            _ ->
                %% we are at the limit, wait iff we would be creating a new queue
                not util:has(Tasks, Name)
        end,
    case Wait of
        true ->
            receive after TaskWaitFor -> enqueue_task(Base, Node, Task, State) end;
        false ->
            util:modify(State, [tasks, Name],
                        fun (undefined) ->
                                {undefined, [{Node, {Task, Base}}]};
                            ({Pid, Stack}) ->
                                {Pid, Stack ++ [{Node, {Task, Base}}]}
                        end)
    end.

handle_task(#{kind := retry, name := Name, value := Arg}, Node, State) ->
    util:swap(State, [tasks, Name],
              fun ({Pid, Stack}) ->
                      %% update the arg, it will be used if we respawn
                      {Pid, util:swap(Stack, Node,
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

complete_task(#{name := Name} = Message, Node, State) ->
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
    loom:task_completed(Message, Node, util:get(Message, value), State1).

spawn_task(_, [{Node, _}|_], _) when Node =/= node() ->
    undefined;
spawn_task(Name, [{_, {{F, Arg}, Base}}|_], State) ->
    Timer = time:timer(),
    Fun =
        case erlang:fun_info(F, arity) of
            {arity, 2} -> fun (_, A, S) -> F(A, S) end;
            {arity, 3} -> F
        end,
    Run =
        fun Loop(N, A, S) ->
                case catch Fun({N, Timer}, A, S) of
                    {retry, Wait, Reason} ->
                        %% if the arg doesn't change there's no need to send a message
                        %% give the user callback a chance to maybe log, or take action
                        loom:task_continued(Name, Reason, {N, Timer}, A, S),
                        receive after time:timeout(Wait) -> Loop(N + 1, A, loom:state(S)) end;
                    {retry, A1, Wait, Reason} ->
                        %% change the arg without sending a message
                        loom:task_continued(Name, Reason, {N, Timer}, A, S),
                        receive after time:timeout(Wait) -> Loop(N + 1, A1, loom:state(S)) end;
                    {renew, A1, Wait, Reason} ->
                        %% if the arg changes we send a message to notify the loom
                        %% if we crash, we must either:
                        %%  1. try the previous arg again right away
                        %%  2. try the next arg right away
                        %%  3. try the next arg with an additional wait penalty
                        %% regardless, we can't avoid the possibility of repeating an arg
                        %% crashing is rare, all options have issues: do something simple (#2)
                        loom:call(S, Base#{type => task, kind => retry, value => A1}),
                        loom:task_continued(Name, Reason, {N, Timer}, A, S),
                        receive after time:timeout(Wait) -> Loop(N + 1, A1, loom:state(S)) end;
                    {done, Result} ->
                        %% when we are done, notify the loom so we can finish the task
                        loom:call(S, Base#{type => task, kind => done, value => Result});
                    Other ->
                        %% user failure happens, treat it like done but different
                        loom:call(S, Base#{type => task, kind => fail, value => Other})
                end
        end,
    spawn_link(fun () -> Run(0, Arg, State) end).

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

tasks_remaining(Node, #{tasks := Tasks}) ->
    maps:fold(fun (_, {_, Stack}, Acc) ->
                      lists:foldl(fun ({N, _}, A) when N =:= Node ->
                                          A + 1;
                                      ({_, _}, A) ->
                                          A
                                  end, Acc, Stack)
              end, 0, Tasks).
