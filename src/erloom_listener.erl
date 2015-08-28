-module(erloom_listener).

-export([spawn/1]).

spawn(Spec) ->
    spawn_link(fun () -> init(Spec) end).

init(Spec) ->
    process_flag(trap_exit, true),
    Listener = self(),
    Worker = erloom_worker:spawn(),
    State = loom:load(#{listener => Listener, worker => Worker, spec => Spec}),
    listen(catchup, State).

listen(catchup, State = #{status := awake, prior := _, emits := Emits}) ->
    %% first, give a chance for an unstable state to emit any messages to itself
    %% this will repeat until we reach a stable state
    case Emits of
        [] ->
            %% no more emissions: attempt to replay logs from point to front
            replay_logs(State);
        [{_Key, Message}|Rest] ->
            %% treat an emission as a new message to self, except noop reply
            work_on({new_message, Message, fun (_) -> ok end}, State#{emits => Rest})
    end;
listen(catchup, State) ->
    %% if this is our first time through, we might not be at our own tip
    %%  dont emit as we may have already done it
    %% otherwise we might be waiting / recovering
    %%  its not safe to write to our log yet
    replay_logs(State);

listen(ready, State = #{opts := Opts, active := Active, tasks := Tasks}) ->
    %% syncing happens when we realize we are missing data
    %% or every so often, as long as we think we're ahead (i.e. retry interval)
    %% idling can only be handled by the listener,
    %% so the periodic sync and idling need to share the main receive timeout
    %% NB: its currently possible to idle even if we have data to sync
    %%     if our peers are inaccessible, we could stay alive... maybe we will
    #{idle_elapsed := IdleElapsed,
      idle_timeout := IdleTimeout,
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
            State1 =
                case SyncElapsed + Timeout of
                    E1 when E1 >= SyncInterval ->
                        erloom_sync:maybe_push(util:modify(State, [opts, sync_initial], time:timer()));
                    _ ->
                        State
                end,
            State2 =
                case IdleElapsed + Timeout of
                    _ when not Active ->
                        loom:sleep(State1);
                    E2 when E2 >= IdleTimeout, map_size(Tasks) =:= 0 ->
                        loom:handle_idle(util:modify(State1, [opts, idle_elapsed], 0));
                    E2 ->
                        util:modify(State1, [opts, idle_elapsed], E2)
                end,
            listen(ready, State2)
    end;

listen(busy, State = #{worker := Worker}) ->
    %% same as above, except we can't process new messages or replay logs while we are busy
    %% so dont handle new messages, and instead of actually catching up, return to busy state
    receive
        {worker_done, _} = Term ->
            heard(busy, Term, State);
        {sync_logs, Packet} = Term ->
            %% inform the worker of the latest fronts, in case its waiting
            Worker ! {sync_logs, maps:with([from, front], Packet)},
            heard(busy, Term, State);
        {get_state, _} = Term ->
            heard(busy, Term, State)
    end.

heard(Phase, Term, State) ->
    react(Phase, Term, util:modify(State, [opts, idle_elapsed], 0)).

react(Phase, {get_state, Reply}, State) ->
    %% get a recent snapshot of the state from any phase (i.e. for tasks or debugging)
    Reply(State),
    listen(Phase, State);

react(ready, {new_message, Message, Reply}, State) ->
    case loom:verify_message(Message, State) of
        {ok, Message1, State1} ->
            work_on({new_message, Message1, Reply}, State1);
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
react(ready, {'EXIT', Worker, Reason}, #{worker := Worker}) ->
    exit(Reason);
react(ready, {'EXIT', _, shutdown}, _State) ->
    exit(shutdown);
react(ready, {'EXIT', _, sleep}, _State) ->
    exit(sleep);
react(ready, {'EXIT', _, silent}, State) ->
    listen(catchup, State);
react(ready, Other, State) ->
    %% during ready phase, all 'other' messages have a chance to do work (e.g. if trapping exits)
    %% we do catchup after, so new state can emit or whatever too
    listen(catchup, loom:handle_info(Other, State));

react(busy, {worker_done, NewState}, State = #{status := awake, front := Front}) ->
    %% if we are awake, listener is the authority on where all the logs are, except our own
    Front1 = maps:merge(Front, maps:with([node()], maps:get(front, NewState))),
    State1 = maps:merge(NewState, maps:with([status, opts, edges], State)),
    State2 = State1#{front => Front1},
    listen(catchup, State2);
react(busy, {worker_done, NewState}, State = #{status := _, front := Front}) ->
    %% if we are not awake yet, listener is the authority on all logs, including our own
    State1 = maps:merge(NewState, maps:with([status, opts, edges], State)),
    State2 = State1#{front => Front},
    listen(catchup, check_recovery(State2));
react(busy, {sync_logs, Packet}, State) ->
    listen(busy, erloom_sync:got_sync(Packet, State)).

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
                    Message = #{deps => #{node() => OurMark}, type => recover},
                    loom:charge_emit(recovered, Message, loom:waken(State));
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
