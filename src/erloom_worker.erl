-module(erloom_worker).

-export([spawn/0]).

spawn() ->
    spawn_link(fun () -> wait() end).

wait() ->
    receive
        {{new_message, Message, Reply}, State} ->
            State1 = write_through(Message, State),
            State2 = loom:pure_effects(Message, node(), State1),
            State3 = point_to_front(node(), State2),
            State4 = loom:side_effects(Message, Reply, State1, State3),
            done(State4);
        {replay_logs, State} ->
            State1 = replay_logs(State),
            done(State1);
        {sync_logs, _} ->
            %% flush extraneous messages while we are not waiting
            wait()
    end.

done(State = #{listener := Listener}) ->
    Listener ! {worker_done, State},
    wait().

point_to_front(Node, State) ->
    util:modify(State, [point, Node], util:lookup(State, [front, Node])).

replay_logs(State = #{front := Front}) ->
    try
        %% try to replay to front, recursively adding deps as needed
        %% recursion depth is practically bound by the number of nodes
        %% if we don't target our own front first, its not guaranteed we will reach it
        replay_logs([maps:with([node()], Front), maps:without([node()], Front)], State)
    catch
        %% if we can't go any further, try to resolve the problem quickly
        %% in the meantime just return as far as we get
        throw:{unreachable, Target, State1} ->
            erloom_sync:maybe_pull(Target, State1)
    end.

replay_logs([Target|Stack], State = #{point := Point, front := Front}) ->
    Replay =
        fun (Message, Node, S) ->
                case loom:unmet_deps(Message, S) of
                    nil ->
                        loom:pure_effects(Message, Node, S);
                    Deps ->
                        replay_logs([Deps, Target], S)
                end
        end,
    State1 =
        case erloom:edge_delta(Target, Point) of
            TP when map_size(TP) > 0 ->
                %% target is ahead of point: try to reach it
                case erloom:edge_delta(Target, Front) of
                    TF when map_size(TF) > 0 ->
                        %% target is unreachable: stop
                        throw({unreachable, Target, State});
                    _ ->
                        %% target is contained in front
                        maps:fold(fun (Node, Range, S) ->
                                          erloom_logs:replay(Replay, Range, Node, S)
                                  end, State, TP)
                end;
            _ ->
                %% target is reached
                State
        end,
    replay_logs(Stack, State1);
replay_logs([], State) ->
    State.

write_through(Message, State) ->
    write_through(loom:write_through(Message, State), Message, State).

write_through(Fun, Message, State = #{peers := Peers}) when is_function(Fun) ->
    write_through(Fun(map_size(Peers)), Message, State);
write_through({W, T}, Message, State = #{peers := Peers, ours := Ours}) when W > 0 ->
    %% write to our own log, and push the entries right away
    {EntryList, State1} = erloom_logs:write(Message, State),
    State2 = erloom_sync:do_push(#{Ours => EntryList}, State1),
    Peers = util:map(Peers, fun (_) -> false end),
    Tip = util:lookup(State2, [front, node()]),
    wait_for({W, T}, 1, Tip, time:timer(), State2#{peers => Peers});
write_through({0, _}, _, State) ->
    State.

wait_for({W, T}, N, Tip, Start, State = #{peers := Peers}) when N < W ->
    %% give the nodes a chance to reply that they've synced up to our tip
    receive
        {sync_logs, #{from := {FromNode, _}, front := Edge}} ->
            case {util:get(Edge, node()), util:has(Peers, FromNode)} of
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
