-module(erloom_worker).

-export([spawn/0]).

spawn() ->
    spawn_link(fun () -> init() end).

init() ->
    process_flag(trap_exit, true),
    wait().

wait() ->
    receive
        {{new_message, Message, Reply}, State} ->
            %% writing to peers before updating is not arbitrary
            %% log prefix tells who could have been written to next
            State1 = write_through(Message, State),
            State2 = loom:pure_effects(Message, node(), State1),
            State3 = point_to_front(node(), State2),
            State4 = loom:side_effects(Message, Reply, State1, State3),
            done(State4);
        {replay_logs, State} ->
            State1 = replay_logs(State),
            done(State1);
        {sync_logs, _} ->
            %% flush extraneous messages while we are not waiting for acks
            wait();
        {'EXIT', _, normal} ->
            %% normal exits trap normally
            wait();
        {'EXIT', _, silent} ->
            %% trap exits to allow children (tasks) to be killed quietly
            wait();
        {'EXIT', _, Reason} ->
            %% all other exits happen as if we weren't trapping
            exit(Reason)
    end.

done(State = #{listener := Listener, status := Status}) ->
    State1 =
        case Status of
            awake ->
                %% only launch tasks when we are sure we are at tip
                erloom_surety:launch_tasks(State);
            _ ->
                %% otherwise we'll launch tasks when we wake up
                State
        end,
    Listener ! {worker_done, State1},
    wait().

point_to_front(Node, State) ->
    util:modify(State, [point, Node], util:lookup(State, [front, Node])).

replay_logs(State = #{front := Front}) ->
    try
        %% try to replay to front, recursively adding deps as needed
        %% recursion depth is practically bound by the number of nodes
        %% if we don't target our own front first, its not guaranteed we will reach it
        Targets = [maps:with([node()], Front), maps:without([node()], Front)],
        erloom_logs:replay(fun loom:pure_effects/3, Targets, State)
    catch
        %% if we can't go any further, try to resolve the problem quickly
        %% in the meantime just return as far as we get
        throw:{unreachable, Target, State1} ->
            erloom_sync:maybe_pull(Target, State1)
    end.

write_through(Message, State = #{peers := Peers}) ->
    write_through(loom:write_through(Message, map_size(Peers) + 1, State), Message, State).

write_through({W, T}, Message, State = #{peers := Peers, ours := Ours}) when W > 0 ->
    %% write to our own log, and push the entries right away
    {EntryList, State1} = erloom_logs:write(Message, State),
    State2 = erloom_sync:do_push(#{Ours => EntryList}, State1),
    Peers1 = util:map(Peers, fun (_) -> false end),
    Tip = util:lookup(State2, [front, node()]),
    wait_for({W, T}, 1, Tip, time:timer(), State2#{peers => Peers1});
write_through({0, _}, _, State) ->
    State.

wait_for({W, T}, N, Tip, Start, State = #{peers := Peers}) when N < W ->
    %% give the nodes a chance to reply that they've synced up to our tip
    receive
        {sync_logs, #{from := {FromNode, _}, front := Edge}} ->
            case {util:get(Edge, node()), util:has(Peers, [FromNode])} of
                {Mark, IsPeer} when Mark < Tip orelse not IsPeer ->
                    wait_for({W, T}, N, Tip, Start, State);
                _ ->
                    Peers1 = Peers#{FromNode => true},
                    State1 = State#{peers => Peers1},
                    wait_for({W, T}, N + 1, Tip, Start, State1)
            end
    after
        time:timer_remaining(T, Start) ->
            State#{wrote => {N, W}}
    end;
wait_for({W, _}, W, _, _, State) ->
    State#{wrote => {W, W}}.
