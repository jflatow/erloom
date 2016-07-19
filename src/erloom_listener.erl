-module(erloom_listener).
-author("Jared Flatow").

-export([spawn/1]).

spawn(Spec) ->
    spawn_link(fun () -> init(Spec) end).

init(Spec) ->
    process_flag(trap_exit, true),
    Listener = self(),
    Worker = erloom_worker:spawn(Listener),
    State = loom:load(#{listener => Listener, worker => Worker, spec => Spec}),
    listen(catchup, State).

listen(catchup, State = #{status := awake, prior := _, emits := Emits}) ->
    %% first, give a chance for an unstable state to emit any messages to itself
    %% this will repeat until we reach a stable state
    case at_tip(State) of
        true ->
            %% we are at our own tip, we can emit
            case Emits of
                [] ->
                    %% no more emissions: attempt to replay logs from point to front
                    replay_logs(State);
                [{_Key, Message}|Rest] ->
                    %% treat an emission as a new message to self
                    work_on({new_message, Message, undefined}, State#{emits => Rest});
                [{_Key, Message, Reply}|Rest] ->
                    %% also allow setting the reply for emits
                    work_on({new_message, Message, Reply}, State#{emits => Rest})
            end;
        false ->
            %% we are not at our tip, keep trying (i.e. we probably just recovered)
            replay_logs(State)
    end;
listen(catchup, State = #{status := waking, point := Front, front := Front}) ->
    %% we're almost awake and we've caught up to front: finish waking up
    listen(catchup, loom:waken(State));
listen(catchup, State = #{status := S, prior := _}) when S =:= waiting; S =:= recovering ->
    %% we're still waiting / recovering: its not safe to write to our log yet
    replay_logs(State);
listen(catchup, State) ->
    %% this is our first time through, we might not be at our own tip
    %% dont emit or start tasks as we may have already done it
    replay_logs(loom:init(State));

listen(ready, State = #{opts := Opts, active := Active}) ->
    %% syncing happens when we realize we are missing data
    %% or every so often, as long as we think we're ahead (i.e. retry interval)
    %% idling can only be handled by the listener,
    %% so the periodic sync and idling need to share the main receive timeout
    %% NB: its currently possible to idle even if we have data to sync
    %%     if our peers are inaccessible, we could stay alive... maybe we will
    #{idle_elapsed := IdleElapsed,
      idle_timeout := IdleTimeout,
      wipe_timeout := WipeTimeout,
      sync_initial := SyncInitial,
      sync_interval := SyncInterval} = Opts,
    SyncElapsed = time:timer_elapsed(SyncInitial),
    IdleRemaining = time:timeout_remaining(IdleTimeout, IdleElapsed),
    SyncRemaining = time:timeout_remaining(SyncInterval, SyncElapsed),
    Timeout = min(IdleRemaining, SyncRemaining),
    receive
        Any ->
            heard(ready, Any, State)
    after
        Timeout ->
            NTasks = erloom_surety:tasks_remaining(node(), State),
            State1 =
                case SyncElapsed + Timeout of
                    E1 when E1 >= SyncInterval ->
                        erloom_sync:maybe_push(util:modify(State, [opts, sync_initial], time:timer()));
                    _ ->
                        State
                end,
            State2 =
                case IdleElapsed + Timeout of
                    E2 when E2 >= IdleTimeout, NTasks =:= 0, Active ->
                        loom:handle_idle(util:modify(State1, [opts, idle_elapsed], 0));
                    E2 when E2 >= WipeTimeout, NTasks =:= 0, not Active ->
                        loom:wipe(State1);
                    E2 when E2 >= IdleTimeout ->
                        util:modify(State1, [opts, idle_elapsed], 0);
                    E2 ->
                        util:modify(State1, [opts, idle_elapsed], E2)
                end,
            listen(ready, State2)
    end;

listen(busy, State = #{worker := Worker}) ->
    %% same as above, except we can't process new messages or replay logs while we are busy
    %% so dont handle new messages, and instead of actually catching up, return to busy state
    %% only handle exits which will not cause us to update state
    receive
        {worker_done, _} = Term ->
            heard(busy, Term, State);
        {sync_logs, Packet} = Term ->
            %% inform the worker of the latest fronts, in case its waiting
            Worker ! {sync_logs, maps:with([from, front], Packet)},
            heard(busy, Term, State);
        {get_state, _} = Term ->
            heard(busy, Term, State);
        {'EXIT', From, Reason} = Term when From =:= Worker; Reason =:= shutdown; Reason =:= sleep ->
            heard(busy, Term, State)
    end.

heard(_, {'EXIT', Worker, Reason}, #{worker := Worker}) ->
    exit(Reason);
heard(_, {'EXIT', _, shutdown}, _State) ->
    exit(shutdown);
heard(_, {'EXIT', _, sleep}, _State) ->
    exit(sleep);
heard(Phase, Term, State) ->
    react(Phase, Term, util:modify(State, [opts, idle_elapsed], 0)).

react(Phase, {get_state, Reply}, State) ->
    %% get a recent snapshot of the state from any phase (i.e. for tasks or debugging)
    Reply(State),
    listen(Phase, State);

react(ready, {new_message, Message, Reply}, State) ->
    case loom:verify_message(Message, State) of
        {ok, Message1, State1 = #{status := awake}} ->
            work_on({new_message, Message1, Reply}, State1);
        {ok, Message1, State1 = #{status := waiting}} ->
            work_on({new_message, Message1, Reply}, loom:waken(State1));
        {incapable, Vsn, State1} ->
            %% we don't have the required code vsn to process the message
            State2 = loom:needs_upgrade(Vsn, State1),
            Reply({retry, {incapable, Vsn}}),
            listen(ready, State2);
        {missing, Edge, State1} ->
            %% point could be behind front, or front could be missing entries
            %% but point was already pushed as far forward as possible before we became ready
            %% either way we haven't met the deps, pull if needed and tell the client to retry
            %% this also guarantees even malicious clients can't create cycles
            State2 = erloom_sync:maybe_pull(Edge, State1),
            Reply({retry, {missing, Edge}}),
            listen(ready, State2);
        {Other, Reason, State1} ->
            Reply({Other, Reason}),
            listen(ready, State1)
    end;
react(ready, {sync_logs, Packet}, State) ->
    %% we got a sync packet: write down entries and reply if needed, then catchup
    listen(catchup, erloom_sync:got_sync(Packet, State));
react(ready, Other, State) ->
    %% during ready phase, all 'other' messages have a chance to do work (e.g. ignored exits)
    %% we do catchup after, so new state can emit or whatever too
    listen(catchup, loom:handle_info(Other, State));

react(busy, {worker_done, NewState}, State = #{status := awake, front := Front}) ->
    %% if we are awake, listener is the authority on where all the logs are, except our own
    Front1 = maps:merge(Front, maps:with([node()], maps:get(front, NewState))),
    State1 = maps:merge(NewState, maps:with([status, opts, logs, edges], State)),
    State2 = State1#{front => Front1},
    listen(catchup, State2);
react(busy, {worker_done, NewState}, State = #{status := _, front := Front}) ->
    %% if we are not awake yet, listener is the authority on all logs, including our own
    State1 = maps:merge(NewState, maps:with([status, opts, logs, edges], State)),
    State2 = State1#{front => Front},
    listen(catchup, check_recovery(State2));
react(busy, {sync_logs, Packet}, State) ->
    listen(busy, erloom_sync:got_sync(Packet, State)).

at_tip(#{point := Point, front := Front}) ->
    util:get(Point, node()) =:= util:get(Front, node()).

check_recovery(State = #{status := recovering, peers := Peers, front := Front, edges := Edges}) ->
    %% keep recovering until:
    %%  - we have at least one entry
    %%  - all of our peers have edges
    %%  - none of them are ahead on *our* logs
    %% this means we wait for all peers to be online before we'll be ready again
    case util:get(Front, node()) of
        undefined ->
            %% no entries, we should at least find a 'start', otherwise we wouldn't be here
            State;
        OurMark ->
            Recovered =
                maps:fold(fun (_, _, false) ->
                                  false;
                              (Peer, _, true) ->
                                  case util:get(Edges, Peer) of
                                      undefined ->
                                          %% no edge for the peer, we can't be sure yet
                                          false;
                                      Edge ->
                                          case util:get(Edge, node()) of
                                              Mark when Mark > OurMark ->
                                                  %% their edge is ahead of our log
                                                  false;
                                              _ ->
                                                  %% this peer looks good
                                                  true
                                          end
                                  end
                          end, true, Peers),
            case Recovered of
                true ->
                    Message = #{
                      deps => #{node() => OurMark},
                      type => recover,
                      yarn => recover
                     },
                    loom:stitch_yarn(Message, loom:waken(State));
                false ->
                    State
            end
    end;
check_recovery(State) ->
    State.

replay_logs(State) ->
    %% only replay if the front has changed since the last time we replayed
    %% this check guarantees that we don't stagnate, even if the point is stuck behind the front
    %% this also means we implicitly prioritize replaying copied messages over receiving new ones
    case State of
        #{front := Front, prior := Front} ->
            listen(ready, check_recovery(State));
        #{front := Front} ->
            work_on(replay_logs, State#{prior => Front})
    end.

work_on(Term, State = #{worker := Worker}) ->
    Worker ! {Term, State},
    listen(busy, State).
