-module(erloom_electorate).

-export([motion/2,
         tether/2,
         handle_motion/3,
         handle_ballot/3,
         handle_ratify/3,
         handle_ctrl/3,
         handle_task/4]).

create(_, _, State = #{elect := _}) ->
    State#{response => already_exists};
create(Electors, Node, State = #{locus := Seed}) ->
    %% the root conf is symbolic, to allow seeding different nodes with the same message
    %% this increases availability during seeding while keeping voting results consistent
    %% i.e. in case seed is received by a node, but it dies before replying
    %%      if we didn't do this we'd have to be sure to wait for the same node to recover
    %% NB: the seed message content must of course be identical between instances
    Change = {'=', Electors},
    State1 = State#{elect => #{
                      seed => Seed,
                      known => root,
                      current => root,
                      pending => #{},
                      root => {[], #{kind => conf, value => Change}, decided}
                     }},
    config(root, Change, Node, State1#{response => ok}).

motion(Motion = #{fiat := _}, State) ->
    %% fiat is forced, must depend on a known conf, or it might not take effect
    %% NB:
    %%  fiats are inherently unsafe, things that were previously true may become untrue
    %%  consistency can only be guaranteed by the voting system when motions are voted on
    %%  fiat decisions are made by the issuer, its up to them to know what they are doing
    Motion1 = Motion#{
                type => motion,
                conf => util:lookup(State, [elect, known])
               },
    loom:stitch_yarn(min_refs(Motion1, State), State);
motion(Motion, State) ->
    %% not a fiat, voting predicated on currently believed conf
    Motion1 = Motion#{
                type => motion,
                conf => util:lookup(State, [elect, current])
               },
    loom:stitch_yarn(min_refs(Motion1, State), State).

tether(Change = #{path := Path}, State) ->
    Change1 = Change#{
                kind => chain,
                prior => erloom_chain:version(State, Path)
               },
    motion(Change1, State).

min_refs(Message = #{conf := root}, State) ->
    %% add the refs, keeping the explicit conf id
    Message#{refs => util:lookup(State, [elect, seed])};
min_refs(Message = #{conf := ConfId}, _) ->
    %% no need to keep the conf when we store it in refs
    maps:without([conf], Message#{refs => ConfId}).

get_conf_id(#{conf := ConfId}) ->
    ConfId;
get_conf_id(#{refs := ConfId}) ->
    ConfId;
get_conf_id(_) ->
    undefined.

get_conf(Motion, State) ->
    get_motion(get_conf_id(Motion), State).

get_motion_id(#{type := motion}, #{locus := Locus}) ->
    %% we are processing the motion, the id is the locus
    Locus;
get_motion_id(#{type := ballot, refs := MotionId}, _State) ->
    %% we are processing the ballot, the id is the ref
    MotionId.

get_motion(MotionId, #{elect := Elect}) ->
    get_motion(MotionId, Elect);
get_motion(MotionId, Elect) ->
    case util:get(Elect, MotionId) of
        undefined ->
            undefined;
        {_, Motion, _} ->
            Motion
    end.

is_descendant(Id, Id, _Elect) ->
    true;
is_descendant(Id, OId, Elect) ->
    case get_conf_id(get_motion(Id, Elect)) of
        undefined ->
            false;
        ConfId ->
            is_descendant(ConfId, OId, Elect)
    end.

get_ancestors(ConfId, #{elect := Elect}) ->
    get_ancestors(ConfId, Elect, []).

get_ancestors(undefined, _, Acc) ->
    Acc;
get_ancestors(ConfId, Elect, Acc) ->
    case util:get(Elect, ConfId) of
        ConfInfo = {_, Motion, _} ->
            get_ancestors(get_conf_id(Motion), Elect, [{ConfId, ConfInfo}|Acc])
    end.

get_children(MotionId, #{elect := Elect}) ->
    case util:get(Elect, MotionId) of
        {Kids, _, _} ->
            [{K, util:get(Elect, K)} || K <- Kids];
        _ ->
            []
    end.

get_siblings(MotionId, State = #{elect := Elect}) ->
    case util:get(Elect, MotionId) of
        undefined ->
            [];
        MotionInfo ->
            get_siblings(MotionId, MotionInfo, State)
    end.

get_siblings(MotionId, {_, Motion, _}, State) ->
    [C || C = {K, _} <- get_children(get_conf_id(Motion), State), K =/= MotionId];
get_siblings(_, _, _) ->
    [].

get_predicates(Motion, #{elect := Elect}) ->
    get_ancestors(get_conf_id(Motion), Elect, []).

get_electorate(Motion, State) ->
    %% walk from root conf to confid and calculate the voting group
    %% NB: not optimized for lots of changes to conf, could cache
    lists:foldl(fun ({_, {_, #{value := Change}, _}}, Acc) ->
                        util:op(Acc, Change)
                end, [], get_predicates(Motion, State)).

list_electorates(ConfId, State) ->
    lists:foldl(fun ({Id, {_, #{value := Change}, _}}, Acc) ->
                        {_, Electorate} = util:first(Acc, {undefined, []}),
                        [{Id, util:op(Electorate, Change)}|Acc]
                end, [], get_ancestors(ConfId, State)).

list_electorates(FromId, ToId, State) ->
    util:range(list_electorates(ToId, State), fun ({Id, _}) -> Id =:= FromId end).

get_electorate_diff(MotionId, Change, State) ->
    Old = get_electorate(get_motion(MotionId, State), State),
    New = util:op(Old, Change),
    {New -- Old, Old -- New}.

get_mover(MotionId) ->
    erloom:locus_node(MotionId).

delete_motion(MotionId, Motion, State = #{elect := Elect}) ->
    %% remove the motion from the parent, and delete the motion
    ConfId = get_conf_id(Motion),
    Elect1 =
        case util:get(Elect, ConfId) of
            undefined ->
                Elect;
            {Kids, Parent, Votes} ->
                util:set(Elect, ConfId, {lists:delete(MotionId, Kids), Parent, Votes})
        end,
    util:set(State, elect, util:remove(Elect1, MotionId)).

has_right_electorate(Motion, #{elect := #{current := ConfId}}) ->
    case get_conf_id(Motion) of
        ConfId ->
            true;
        _ ->
            false
    end.

needs_vote(MotionId, Node, State) ->
    case util:lookup(State, [elect, MotionId]) of
        {_, _, Votes} when is_map(Votes) ->
            not util:has(Votes, Node);
        _ ->
            false
    end.

vote(MotionId, _Motion, Vote, State) ->
    Ballot = #{
      refs => MotionId,
      type => ballot,
      vote => Vote
     },
    loom:stitch_yarn(Ballot, State).

ratify(MotionId, _Motion, State) ->
    Ratification = #{
      refs => MotionId,
      type => ratify
     },
    loom:suture_yarn(Ratification, State).

handle_motion(Motion, Mover, State) ->
    MotionId = get_motion_id(Motion, State),
    State1 = maybe_save_motion(Motion, MotionId, State),
    State2 = maybe_save_pending(Motion, MotionId, State1),
    State3 = maybe_vote_stopped(Motion, MotionId, State2),
    State4 =
        case util:lookup(State3, [elect, MotionId]) of
            undefined ->
                %% we didn't save, must have been predicated on an impossible electorate
                resolve_motion(MotionId, Motion, Mover, {false, premise}, State3);
            _ ->
                %% we saved, we should maybe try to vote on it
                NeedsVote = needs_vote(MotionId, node(), State3),
                case util:get(Motion, fiat) of
                    undefined when NeedsVote ->
                        %% not a fiat, normal voting
                        case lists:member(node(), get_electorate(Motion, State3)) of
                            true ->
                                %% we are part of the motion's electorate, vote one way or another
                                case has_right_electorate(Motion, State3) of
                                    true ->
                                        %% the motion has the 'right' electorate
                                        {Vote, S} = vote_on_motion(MotionId, Motion, Mover, State3),
                                        vote(MotionId, Motion, Vote, S);
                                    false ->
                                        %% we don't agree about the electorate
                                        vote(MotionId, Motion, {nay, electorate}, State3)
                                end;
                            false ->
                                %% not part of the motion's electorate, our vote won't count anyway
                                State3
                        end;
                    undefined ->
                        %% not a fiat, but we don't need to vote (probably we are stopped already)
                        State3;
                    _ ->
                        %% its a fiat, no need to vote
                        State3
                end
        end,
    adjudicate(Motion, MotionId, State4).

handle_ballot(Ballot, Node, State) ->
    MotionId = get_motion_id(Ballot, State),
    State1 = maybe_save_ballot(Ballot, MotionId, Node, State),
    adjudicate(Ballot, MotionId, State1).

handle_ratify(#{refs := MotionId}, Node, State) ->
    case get_motion(MotionId, State) of
        undefined ->
            %% most motions are deleted after they are decided anyway
            %% nothing generic to do if its not a conf (and / or deleted)
            State;
        #{kind := conf, value := Change} ->
            %% conf change has been ratified, start taking action
            config(MotionId, Change, Node, State)
    end.

handle_ctrl(#{type := start, seed := Nodes}, Node, State) ->
    %% the seed message sets the initial electorate, and thus the peer group
    %% every node in the group should see / refer to the same seed message
    create(Nodes, Node, State);
handle_ctrl(Ctrl = #{type := Type}, Node, State) ->
    %% we keep a list of nodes that need start / stop for each config task
    %% when we see a node successfully started / stopped, we update the list
    %% the stop message also implies 'nay' on certain motions (see imply_votes)
    State1 =
        util:replace(State, [elect, pending, {conf, get_conf_id(Ctrl)}],
                     fun ({Start, Stop}) when Type =:= start ->
                             {lists:delete(Node, Start), Stop};
                         ({Start, Stop}) when Type =:= stop ->
                             {Start, lists:delete(Node, Stop)};
                         (Other) ->
                             Other
                     end),
    State2 = imply_votes(Ctrl, Node, State1),
    State2#{response => ok}.

handle_task(#{name := config}, Node, ConfId, State) ->
    case get_motion(ConfId, State) of
        undefined ->
            %% if the conf somehow disappears (maybe due to a fiat?), just do nothing
            State;
        #{kind := conf, value := Change} ->
            done_config(ConfId, Change, Node, State)
    end.

maybe_save_motion(Motion, MotionId, State = #{elect := Elect}) ->
    ConfId = get_conf_id(Motion),
    case util:get(Elect, ConfId) of
        undefined ->
            %% the conf has been forgotten, it must be an impossible electorate
            State;
        {Kids, CMotion, CVotes} ->
            %% the conf exists, its a possibility
            State#{elect => Elect#{
                              MotionId => {[], Motion, #{}},
                              ConfId => {[MotionId|Kids], CMotion, CVotes}
                             }}
    end.

maybe_save_ballot(#{vote := Vote}, MotionId, Node, State = #{elect := Elect}) ->
    case util:get(Elect, MotionId) of
        undefined ->
            %% the motion has been forgotten, it must have already been decided
            State;
        {_, _, decided} ->
            %% the motion is already decided (must be a conf change, as its still here)
            State;
        {Kids, Motion, Votes} when is_map(Votes) ->
            %% the motion is open, register the vote
            State#{elect => Elect#{MotionId => {Kids, Motion, Votes#{Node => Vote}}}}
    end;
maybe_save_ballot(#{fiat := _}, _, _, State) ->
    %% must be a fiat, in which case votes won't matter
    State.

maybe_save_pending(#{kind := chain, path := Path}, MotionId, State) ->
    util:accrue(State, [elect, pending, {chain, Path}], {'+', [MotionId]});
maybe_save_pending(_Motion, _MotionId, State) ->
    State.

maybe_drop_pending(Key, MotionId, State) ->
    State1 = util:accrue(State, [elect, pending, Key], {'-', [MotionId]}),
    util:sluice(State1, [elect, pending, Key], []).

maybe_voted(MotionId, Node, Vote, State) ->
    %% apply vote for node if the motion is open and the node hasn't yet voted
    util:replace(State, [elect, MotionId],
                 fun ({Kids, Motion, Votes}) when is_map(Votes) ->
                         {Kids, Motion, util:ifndef(Votes, Node, Vote)};
                     ({Kids, Motion, Other}) ->
                         {Kids, Motion, Other}
                 end).

maybe_vote_stopped(Motion, MotionId, State) ->
    %% if a new motion is not dependent on the current conf
    %% vote 'nay' for every node in its conf that has been stopped since
    ConfId = get_conf_id(Motion),
    case util:lookup(State, [elect, current]) of
        ConfId ->
            %% it depends on the current conf, nothing to do
            State;
        Current ->
            %% gather every node that has been stopped since the motion
            %% that is, those that were supposed to be stopped and were
            Stopped =
                util:pair(fun ({{_, A}, {Id, B}}, Acc) ->
                                  {_Start, Stop} = util:diff(A, B),
                                  {_, Unstopped} = util:lookup(State, [elect, pending, {conf, Id}], {[], []}),
                                  ordsets:union(Acc, lists:usort(Stop -- Unstopped))
                          end, [], list_electorates(ConfId, Current, State)),
            lists:foldl(fun (Node, S) ->
                                maybe_voted(MotionId, Node, {nay, stopped}, S)
                        end, State, Stopped)
    end.

imply_votes(Ctrl = #{type := stop}, Node, State) ->
    %% the stop message implies 'nay' to any motions which:
    %%  - are dependent on any previous conf
    %%    i.e. child motions of every conf previous to the one referenced when stopped
    %%  - the node did not vote on prior to its stop message
    %% this must hold true not only for motions which we have seen so far on this node
    %% but also for motions which we haven't seen yet that fit the above criteria
    lists:foldl(
      fun ({_, {Kids, _, _}}, S) ->
              lists:foldl(fun (Kid, S1) ->
                                  maybe_voted(Kid, Node, {nay, stopped}, S1)
                          end, S, Kids)
      end, State, get_predicates(get_conf(Ctrl, State), State));
imply_votes(_, _, State) ->
    State.

adjudicate(Ballot, MotionId, State = #{elect := Elect}) ->
    %% if the electorate is valid, and the motion is decided, declare decision
    case util:get(Elect, MotionId) of
        undefined ->
            %% the motion has already been decided
            State;
        MotionInfo = {_, Motion, _} ->
            %% the motion is still a possibility (it exists, so its open)
            case util:get(Elect, get_conf_id(Motion)) of
                {_, _, decided} ->
                    %% the electorate is known to be valid
                    case util:get(Ballot, fiat) of
                        undefined ->
                            %% not a fiat, run a normal election process
                            reckon_elections(MotionId, MotionInfo, State);
                        Decision ->
                            %% its a fiat, just reflect the decision
                            reflect_decision(MotionId, MotionInfo, Decision, State)
                    end;
                {_, _, _} ->
                    %% the electorate is not yet determined
                    State
            end
    end.

reckon_elections(_, {_, _, decided}, State) ->
    %% do nothing if the decision is already made
    State;
reckon_elections(MotionId, MotionInfo, State) ->
    %% presume the electorate is correct, tally votes on the motion and reflect if needed
    %% keeps going if a decision has been made (mutually recursive with reflect_decision)
    case tally_votes(MotionId, MotionInfo, State) of
        undefined ->
            State;
        Decision ->
            reflect_decision(MotionId, MotionInfo, Decision, State)
    end.

reflect_decision(MotionId, {Kids, Motion, _}, Decision = {true, _}, State = #{elect := Elect}) ->
    %% notify the loom, and recursively determine any dependent motions
    Mover = get_mover(MotionId),
    State1 =
        case Motion of
            #{kind := conf} ->
                %% the motion was a conf change, update the last known conf and mark decided
                %% if the current belief is not a descendant of known, change it
                %% any conf siblings must be killed, there can only be one
                %% finally, if we are the mover, ratify the motion
                MotionInfo = {Kids, Motion, decided},
                Elect1 = Elect#{known => MotionId, MotionId => MotionInfo},
                Elect2 =
                    case is_descendant(util:get(Elect1, current), MotionId, Elect1) of
                        true ->
                            Elect1;
                        false ->
                            Elect1#{current => MotionId}
                    end,
                case murder_conf_siblings(MotionId, State#{elect => Elect2}) of
                    S when Mover =:= node() ->
                        ratify(MotionId, Motion, S);
                    S ->
                        S
                end;
            _ ->
                %% the motion was not a conf change, no need to keep it around
                delete_motion(MotionId, Motion, State)
        end,
    State2 = resolve_motion(MotionId, Motion, Mover, Decision, State1),
    lists:foldl(fun (Kid, S = #{elect := E}) ->
                        reckon_elections(Kid, util:get(E, Kid), S)
                end, State2, Kids);
reflect_decision(MotionId, {Kids, Motion, _}, Decision, State = #{elect := Elect}) ->
    %% notify the loom, and recursively reflect any dependent motions
    %% no need to keep any definitely false motions
    Mover = get_mover(MotionId),
    State1 =
        case util:get(Elect, current) of
            MotionId ->
                %% we believed the failed motion was the current conf, revert to last known
                State#{elect => Elect#{current => util:get(Elect, known)}};
            _ ->
                %% not the current conf and we didn't believe it was
                State
        end,
    State2 = delete_motion(MotionId, Motion, State1),
    State3 = resolve_motion(MotionId, Motion, Mover, Decision, State2),
    lists:foldl(fun (Kid, S = #{elect := E}) ->
                        reflect_decision(Kid, util:get(E, Kid), {false, premise}, S)
                end, State3, Kids).

vote_on_motion(MotionId, #{kind := conf}, _, State) ->
    %% as soon as we accept a conf change, we update our current (believed) conf
    %% we won't be able to accept any alternative conf change now, only one that depends on this one
    {{yea, ok}, util:modify(State, [elect, current], MotionId)};
vote_on_motion(MotionId, #{kind := chain, path := Path, prior := Prior} = Motion, Mover, State) ->
    case erloom_chain:lookup(State, Path) of
        {_, _, _} ->
            %% the path is locked, cannot accept
            {{nay, locked}, State};
        {_, Prior} ->
            %% the prior matches, lock if accepted
            case loom:vote_on_motion(Motion, Mover, State) of
                {{yea, _} = Vote, S} ->
                    {Vote, erloom_chain:lock(S, Path, MotionId)};
                {Vote, S} ->
                    {Vote, S}
            end;
        {_, _} ->
            %% the prior doesn't match, cannot accept
            {{nay, version}, State}
    end;
vote_on_motion(_, Motion, Mover, State) ->
    loom:vote_on_motion(Motion, Mover, State).

resolve_motion(MotionId, #{kind := chain, path := Path} = Motion, Mover, Decision, State) ->
    State1 = erloom_chain:unlock(State, Path, MotionId),
    State2 = maybe_drop_pending({chain, Path}, MotionId, State1),
    State3 =
        case Decision of
            {true, _} ->
                %% passed a motion to chain: treat as a command now
                loom:cmd(Motion#{version => MotionId}, State2);
            _ ->
                %% failed to chain
                State2#{response => {error, Decision}}
        end,
    motion_decided(Motion, Mover, Decision, State3);
resolve_motion(_MotionId, Motion, Mover, Decision, State) ->
    State1 =
        case Decision of
            {true, _} ->
                State#{response => {ok, Decision}};
            _ ->
                State#{response => {error, Decision}}
        end,
    motion_decided(Motion, Mover, Decision, State1).

motion_decided(Motion, Mover, Decision, State) ->
    loom:motion_decided(Motion, Mover, Decision, State).

tally_votes(_MotionId, {_, Motion, Votes}, State) ->
    case lists:foldl(fun (Member, {N, P, F}) ->
                             case util:get(Votes, Member) of
                                 undefined ->
                                     {N + 1, P, F};
                                 {yea, _} ->
                                     {N + 1, P + 1, F};
                                 {nay, _} ->
                                     {N + 1, P, F + 1}
                             end
                     end, {0, 0, 0}, get_electorate(Motion, State)) of
        {N, P, _} when P > N / 2 ->
            {true, majority};
        {N, _, F} when F >= N / 2 ->
            {false, majority};
        _ ->
            undefined
    end.

murder_conf_siblings(ConfId, State) ->
    lists:foldl(fun ({Id, Info = {_, #{kind := conf}, _}}, S) ->
                        reflect_decision(Id, Info, {false, romulus}, S);
                    (_, S) ->
                        S
                end, State, get_siblings(ConfId, State)).

config(ConfId, Change, Node, State) ->
    %% we *add* peers when the conf is ratified (i.e. before nodes are started)
    %% otherwise nodes won't get the deps they need in order to start
    %% store pending under locus so we can keep track of what's been started / stopped
    %% create the config task
    %%   only the node that makes the motion to change will try to start / stop the nodes
    %%   this is a single point of failure for changing the group (and all tasks) currently
    %%   in theory other nodes could try to take over a task if they think something is wrong
    %%   on the other hand all nodes are eventually supposed to recover, so tasks can't fail
    %%   the tasks are run under a common name, ensuring a total order
    %% initiate a sync right away to cut down on chance that deps will be missing
    Pending = {Add, _} = get_electorate_diff(ConfId, Change, State),
    State1 = util:modify(State, [elect, pending, {conf, ConfId}], Pending),
    State2 = loom:change_peers({'+', Add}, State1),
    State3 = erloom_sync:maybe_push(State2),
    State4 = loom:suture_task(config, Node, {fun do_config/2, ConfId}, State3),
    case Node of
        N when N =:= node() ->
            %% its ours
            State4;
        _ when Add =:= [] ->
            %% not ours, no need to fence when nothing was added
            State4;
        _ when ConfId =:= root ->
            %% not ours, no need to fence the root, everyone must have it at least when they start
            State4;
        _ ->
            %% not ours, just emit a fence marking the peer change
            loom:stitch_yarn(#{type => fence, kind => conf}, State4)
    end.

do_config(ConfId, State = #{spec := Spec}) ->
    %% start / stop nodes that are added / removed
    %% the first try may reach the targets before the deps and fail
    %% its fine, we'll just retry, these messages are relatively uncommon anyway
    case util:lookup(State, [elect, pending, {conf, ConfId}]) of
        {[], []} ->
            {done, ConfId};
        {Start, Stop} ->
            %% give the sync a little bit of a chance to work by waiting a sec
            %% optimize by using responses in addition to the sync channel
            receive after 1000 -> ok end,
            A = loom:multirpc(Start, Spec, min_refs(#{conf => ConfId, type => start}, State)),
            B = loom:multirpc(Stop, Spec, min_refs(#{conf => ConfId, type => stop}, State)),
            case {Start -- util:keys(A, ok), Stop -- util:keys(B, ok)} of
                {[], []} ->
                    {done, ConfId};
                Pending ->
                    {retry, {10, seconds}, {wait, Pending}}
            end
    end.

done_config(ConfId, Change, Node, State) ->
    %% we *remove* peers after the task completes (i.e. after nodes are stopped)
    %% otherwise its possible a node being stopped won't receive its start before being removed
    %% in that case it will be impossible to stop as it will reject everything except the start
    {_, Remove} = get_electorate_diff(ConfId, Change, State),
    State1 = util:remove(State, [elect, pending, {conf, ConfId}]),
    State2 = loom:change_peers({'-', Remove}, State1),
    case Node of
        N when N =:= node() ->
            %% its ours, we're done
            State2;
        _ when Remove =:= [] ->
            %% not ours, no need to fence when nothing was removed
            State2;
        _ ->
            %% not ours, just emit a fence marking the peer change
            loom:stitch_yarn(#{type => fence, kind => conf}, State2)
    end.
