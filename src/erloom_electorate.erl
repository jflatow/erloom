-module(erloom_electorate).

-export([create/3,
         motion/2,
         tether/2,
         handle_motion/3,
         handle_ballot/3,
         handle_ratify/3,
         handle_ctrl/3,
         handle_task/4]).

create(_, _, State = #{elect := _}) ->
    State; %% electorate already exists
create(Electors, Node, State = #{locus := Root}) ->
    Change = {set, Electors},
    State1 = State#{elect => #{
                      root => Root,
                      known => Root,
                      current => Root,
                      pending => #{},
                      Root => {[], #{kind => conf, value => Change}, decided}
                     }},
    config(Root, Change, Node, true, State1).

motion(Motion = #{fiat := _}, State) ->
    %% fiat is forced, must depend on a known conf, or it might not take effect
    %% NB:
    %%  fiats are inherently unsafe, things that were previously true may become untrue
    %%  consistency can only be guaranteed by the voting system when motions are voted on
    %%  fiat decisions are made by the issuer, its up to them to know what they are doing
    Motion1 = Motion#{
                type => motion,
                refs => util:lookup(State, [elect, known])
               },
    loom:create_yarn(Motion1, State);
motion(Motion, State) ->
    %% not a fiat, voting predicated on currently believed conf
    Motion1 = Motion#{
                type => motion,
                refs => util:lookup(State, [elect, current])
               },
    {Motion2, State1} =
        case util:get(Motion1, vote) of
            undefined ->
                {Vote, S} = vote_on_motion(Motion1, node(), State),
                {Motion1#{vote => Vote}, S};
            _ ->
                {Motion1, State}
        end,
    loom:create_yarn(Motion2, State1).

tether(Change = #{path := Path}, State) ->
    Change1 = Change#{
                kind => chain,
                prior => erloom_chain:version(State, Path)
               },
    motion(Change1, State).

change_list({add, Items}, List) ->
    Items ++ List;
change_list({remove, Items}, List) ->
    List -- Items;
change_list({set, Items}, _) ->
    Items.

is_descendant(Id, Id, _Elect) ->
    true;
is_descendant(Id, OId, Elect) ->
    case util:get(Elect, Id) of
        {_, #{refs := ParentId}, _} ->
            is_descendant(ParentId, OId, Elect);
        _->
            false
    end.

get_ancestors(#{refs := ParentId}, #{elect := Elect}) ->
    get_ancestors(ParentId, Elect, []);
get_ancestors(_Motion, _State) ->
    [].

get_ancestors(MotionId, Elect, Acc) ->
    case util:get(Elect, MotionId) of
        MotionInfo = {_, #{refs := ParentId}, _} ->
            get_ancestors(ParentId, Elect, [{MotionId, MotionInfo}|Acc]);
        MotionInfo ->
            [{MotionId, MotionInfo}|Acc]
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

get_siblings(MotionId, {_, #{refs := Parent}, _}, State) ->
    [C || C = {K, _} <- get_children(Parent, State), K =/= MotionId];
get_siblings(_, _, _) ->
    [].

get_electorate(Motion, State) ->
    %% walk from root conf to confid and calculate the voting group
    %% NB: not optimized for lots of changes to conf, could cache
    lists:foldl(fun ({_, {_, #{value := Change}, _}}, Acc) ->
                        change_list(Change, Acc)
                end, [], get_ancestors(Motion, State)).

get_electorate_diff(MotionId, Change, State) ->
    Old = get_electorate(get_motion(MotionId, State), State),
    New = change_list(Change, Old),
    {New -- Old, Old -- New}.

get_motion(MotionId, State) ->
    case util:lookup(State, [elect, MotionId]) of
        undefined ->
            undefined;
        {_, Motion, _} ->
            Motion
    end.

get_motion_id(#{type := motion}, #{locus := Locus}) ->
    %% we are processing the motion, the id is the locus
    Locus;
get_motion_id(#{type := ballot, refs := MotionId}, _State) ->
    %% we are processing the ballot, the id is the ref
    MotionId.

get_mover(MotionId) ->
    erloom:locus_node(MotionId).

delete_motion(MotionId, #{refs := ParentId}, State = #{elect := Elect}) ->
    %% remove the motion from the parent, and delete the motion
    Elect1 =
        case util:get(Elect, ParentId) of
            undefined ->
                Elect;
            {Kids, Parent, Votes} ->
                util:set(Elect, ParentId, {lists:delete(MotionId, Kids), Parent, Votes})
        end,
    util:set(State, elect, util:remove(Elect1, MotionId)).

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

handle_motion(Motion = #{refs := ConfId}, Mover, State) ->
    MotionId = get_motion_id(Motion, State),
    State1 = maybe_save_motion(Motion, MotionId, State),
    case util:lookup(State1, [elect, MotionId]) of
        undefined ->
            %% we didn't save, must have been predicated on an impossible electorate
            resolve_motion(MotionId, Motion, Mover, {false, premise}, State1);
        _ when Mover =:= node() ->
            %% its our motion, no need to emit if we were going to
            handle_ballot(Motion, Mover, State1);
        _ ->
            %% not our motion, we should maybe try to vote on it
            State2 =
                case util:get(Motion, fiat) of
                    undefined ->
                        %% not a fiat, normal voting
                        case lists:member(node(), get_electorate(Motion, State)) of
                            true ->
                                %% we are part of the motion's electorate, vote one way or another
                                case util:lookup(State1, [elect, current]) of
                                    ConfId ->
                                        %% the motion has the 'right' electorate
                                        {Vote, S} = vote_on_motion(Motion, Mover, State1),
                                        vote(MotionId, Motion, Vote, S);
                                    _ ->
                                        %% we don't agree about the electorate
                                        vote(MotionId, Motion, {nay, electorate}, State1)
                                end;
                            false ->
                                %% not part of the motion's electorate, our vote won't count anyway
                                State1
                        end;
                    _ ->
                        %% its a fiat, no need to vote
                        State1
                end,
            handle_ballot(Motion, Mover, State2)
    end.

handle_ballot(Ballot, Node, State) when Node =:= node() ->
    %% its our ballot, no need to emit if we were going to
    %% if the motion was a conf change, we might need to apply it
    MotionId = get_motion_id(Ballot, State),
    State1 = maybe_save_ballot(Ballot, MotionId, Node, State),
    State2 = maybe_change_conf(Ballot, MotionId, State1),
    adjudicate(Ballot, MotionId, State2);
handle_ballot(Ballot, Node, State) ->
    %% its not our ballot, we'll count it in a sec (maybe)
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
            config(MotionId, Change, Node, false, State)
    end.

handle_ctrl(#{type := Type, refs := ConfId}, Node, State) ->
    %% we keep a list of nodes that need start / stop for each config task
    %% when we see a node successfully started / stopped, we update the list
    util:replace(State, [elect, pending, ConfId],
                 fun ({Start, Stop}) when Type =:= start ->
                         {lists:delete(Node, Start), Stop};
                     ({Start, Stop}) when Type =:= stop ->
                         {Start, lists:delete(Node, Stop)};
                     (Other) ->
                         Other
                 end).

handle_task(#{name := config}, Node, ConfId, State) ->
    case get_motion(ConfId, State) of
        undefined ->
            %% if the conf somehow disappears (maybe due to a fiat?), just do nothing
            State;
        #{kind := conf, value := Change} ->
            done_config(ConfId, Change, Node, State)
    end.

maybe_save_motion(Motion = #{refs := ConfId}, MotionId, State = #{elect := Elect}) ->
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

maybe_change_conf(#{vote := {yea, _}}, MotionId, State = #{elect := Elect}) ->
    case util:get(Elect, MotionId) of
        {_, #{kind := conf}, _} ->
            %% the motion is a conf change, and we accepted it, so update current
            State#{elect => Elect#{current => MotionId}};
        _ ->
            %% the motion is not a conf change, leave it
            State
    end;
maybe_change_conf(_, _, State) ->
    %% could be a fiat, in which case change will happen when decision is reflected
    %% otherwise we didn't accept the change
    State.

adjudicate(Ballot, MotionId, State = #{elect := Elect}) ->
    %% if the electorate is valid, and the motion is decided, declare decision
    case util:get(Elect, MotionId) of
        undefined ->
            %% the motion has already been decided
            State;
        MotionInfo = {_, #{refs := ConfId}, _} ->
            %% the motion is still a possibility (it exists, so its open)
            case util:get(Elect, ConfId) of
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

vote_on_motion(#{kind := conf}, _, State) ->
    {{yea, ok}, State};
vote_on_motion(#{kind := chain, path := Path, prior := Prior}, _, State) ->
    case erloom_chain:lookup(State, Path) of
        {_, _, locked} ->
            %% the path is locked, cannot accept
            {{nay, locked}, State};
        {_, Prior} ->
            %% the prior matches, accept by locking
            {{yea, ok}, erloom_chain:lock(State, Path)};
        {_, _} ->
            %% the prior doesn't match, cannot accept
            {{nay, version}, State}
    end;
vote_on_motion(Motion, Mover, State) ->
    loom:vote_on_motion(Motion, Mover, State).

resolve_motion(MotionId, #{kind := chain, path := Path} = Motion, Mover, Decision, State) ->
    State1 =
        case Decision of
            {true, _} ->
                %% passed a motion to chain: store value and bump version
                Value = util:get(Motion, value),
                erloom_chain:modify(State, Path, {Value, MotionId});
            _ ->
                %% failed to chain: unlock
                erloom_chain:unlock(State, Path, MotionId)
        end,
    motion_decided(Motion, Mover, Decision, State1);
resolve_motion(_MotionId, Motion, Mover, Decision, State) ->
    motion_decided(Motion, Mover, Decision, State).

motion_decided(Motion = #{retry := true}, Mover, Decision = {false, _}, State) when Mover =:= node() ->
    %% if the caller expects the motion to eventually pass (i.e. transient failures)
    %% it can use retry / limit to have the mover reissue failed motions automatically
    %% in general we should suture (not stitch) from 'motion_decided'
    %% otherwise replay of emit could occur before the decision, causing re-emit
    State1 =
        case util:get(Motion, limit) of
            undefined ->
                loom:suture_yarn(Motion#{type => move}, State);
            N when is_integer(N), N > 0 ->
                loom:suture_yarn(Motion#{type => move, limit => N - 1}, State);
            _ ->
                State
        end,
    loom:motion_decided(Motion, Mover, Decision, State1);
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

config(ConfId, Change, Node, IsRoot, State) ->
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
    State1 = util:modify(State, [elect, pending, ConfId], Pending),
    State2 = loom:change_peers({add, Add}, State1),
    State3 = erloom_sync:maybe_push(State2),
    State4 = loom:suture_task(config, Node, {fun do_config/2, ConfId}, State3),
    case Node of
        N when N =:= node() ->
            %% its ours
            State4;
        _ when Add =:= [] ->
            %% not ours, no need to fence when nothing was added
            State4;
        _ when IsRoot ->
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
    case util:lookup(State, [elect, pending, ConfId]) of
        {[], []} ->
            {done, ConfId};
        {Start, Stop} ->
            %% give the sync a little bit of a chance to work by waiting a sec
            %% optimize by using responses in addition to the sync channel
            receive after 1000 -> ok end,
            A = loom:multicall(Start, [Spec, #{refs => ConfId, type => start}]),
            B = loom:multicall(Stop, [Spec, #{refs => ConfId, type => stop}]),
            case {Start -- util:keys(A, ok), Stop -- util:keys(B, ok)} of
                {[], []} ->
                    {done, ConfId};
                {_, _} ->
                    {retry, {10, seconds}}
            end
    end.

done_config(ConfId, Change, Node, State) ->
    %% we *remove* peers after the task completes (i.e. after nodes are stopped)
    %% otherwise its possible a node being stopped won't receive its start before being removed
    %% in that case it will be impossible to stop as it will reject everything except the start
    {_, Remove} = get_electorate_diff(ConfId, Change, State),
    State1 = util:remove(State, [elect, pending, ConfId]),
    State2 = loom:change_peers({remove, Remove}, State1),
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
