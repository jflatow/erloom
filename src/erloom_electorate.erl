-module(erloom_electorate).

-export([create/2,
         motion/2,
         affirm_config/3,
         handle_motion/3,
         handle_ballot/3]).

create(_, State = #{elect := _}) ->
    State; %% electorate already exists
create(Electors, State) ->
    Edge = loom:after_locus(State),
    Info = {[], #{kind => conf, conf => {set, Electors}}, {ratified, undefined}},
    State1 = State#{elect => #{
                      root => Edge,
                      known => Edge,
                      current => Edge,
                      Edge => Info
                     }},
    update_conf_peers(Edge, Info, State1).

motion(Motion = #{fiat := _}, State) ->
    %% fiat is forced, must depend on a known conf, or it might not take effect
    %% NB:
    %%  fiats are inherently unsafe, things that were previously true may become untrue
    %%  consistency can only be guaranteed by the voting system when motions are voted on
    %%  fiat decisions are made by the issuer, its up to them to know what they are doing
    Motion1 = Motion#{
                type => motion,
                deps => util:lookup(State, [elect, known])
               },
    loom:emit_message(Motion1, State);
motion(Motion, State) ->
    %% not a fiat, voting predicated on currently believed conf
    Motion1 = Motion#{
                type => motion,
                deps => util:lookup(State, [elect, current])
               },
    {Motion2, State1} =
        case util:get(Motion1, vote) of
            undefined ->
                {Vote, S} = vote_on_motion(Motion1, node(), State),
                {Motion1#{vote => Vote}, S};
            _ ->
                {Motion1, State}
        end,
    loom:emit_message(Motion2, State1).

is_descendant(Id, Id, _Elect) ->
    true;
is_descendant(Id, OId, Elect) ->
    case util:get(Elect, Id) of
        {_, #{deps := ParentId}, _} ->
            is_descendant(ParentId, OId, Elect);
        _->
            false
    end.

get_parent(#{deps := ParentId}, #{elect := Elect}) ->
    util:get(Elect, ParentId);
get_parent(_Motion, _State) ->
    undefined.

get_ancestors(#{deps := ParentId}, #{elect := Elect}) ->
    get_ancestors(ParentId, Elect, []);
get_ancestors(_Motion, _State) ->
    [].

get_ancestors(MotionId, Elect, Acc) ->
    case util:get(Elect, MotionId) of
        MotionInfo = {_, #{deps := ParentId}, _} ->
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

get_siblings(MotionId, {_, #{deps := Parent}, _}, State) ->
    [C || C = {K, _} <- get_children(Parent, State), K =/= MotionId];
get_siblings(_, _, _) ->
    [].

get_electorate(Motion, State) ->
    %% walk from root conf to confid and calculate the voting group
    %% NB: not optimized for lots of changes to conf, could cache
    lists:foldl(fun ({_, {_, #{conf := {add, Electors}}, _}}, Acc) ->
                        Electors ++ Acc;
                    ({_, {_, #{conf := {remove, Electors}}, _}}, Acc) ->
                        Acc -- Electors;
                    ({_, {_, #{conf := {set, Electors}}, _}}, _) ->
                        Electors
                end, [], get_ancestors(Motion, State)).

get_motion_id(#{type := motion}, State) ->
    %% we are processing the motion, the id is the after edge
    loom:after_locus(State);
get_motion_id(#{type := ballot, deps := MotionId}, _State) ->
    %% we are processing the ballot, the id is the deps edge
    MotionId.

get_mover(MotionId) ->
    %% somewhat fragile, but the id currently contains the node that made the motion
    hd(maps:keys(MotionId)).

delete_motion(MotionId, #{deps := ParentId}, State = #{elect := Elect}) ->
    %% remove the motion from the parent, and delete the motion
    Elect1 =
        case util:get(Elect, ParentId) of
            undefined ->
                Elect;
            {Kids, Parent, Votes} ->
                util:set(Elect, ParentId, {lists:delete(MotionId, Kids), Parent, Votes})
        end,
    util:set(State, elect, util:remove(Elect1, MotionId)).

vote(MotionId, Vote, State) ->
    Ballot = #{
      deps => MotionId,
      type => ballot,
      vote => Vote
     },
    loom:emit_message(Ballot, State).

affirm_config(#{type := Type, deps := ConfId}, Node, State) ->
    %% when a conf changes the peer group, we keep a list of nodes that need start / stop
    %% when we see that a node references the conf, with a start or stop, we remove it from the list
    case util:lookup(State, [elect, ConfId]) of
        {Kids, Conf, {ratified, {Start, Stop}}} ->
            Pending =
                case Type of
                    start ->
                        {lists:delete(Node, Start), Stop};
                    stop ->
                        {Start, lists:delete(Node, Stop)}
                end,
            util:modify(State, [elect, ConfId], {Kids, Conf, {ratified, Pending}});
        _ ->
            %% if the conf somehow disappears (maybe a fiat?), just do nothing
            State
    end.

handle_motion(Motion, Node, State) when Node =:= node() ->
    %% its our motion, no need to emit if we were going to
    MotionId = get_motion_id(Motion, State),
    State1 = maybe_save_motion(Motion, MotionId, State),
    handle_ballot(Motion, Node, State1);
handle_motion(Motion = #{deps := ConfId}, Node, State) ->
    %% not our motion, we should maybe try to vote on it
    MotionId = get_motion_id(Motion, State),
    Mover = get_mover(MotionId),
    State1 = maybe_save_motion(Motion, MotionId, State),
    case util:lookup(State1, [elect, MotionId]) of
        undefined ->
            %% the motion must have been predicated on an impossible electorate
            motion_decided(MotionId, Motion, Mover, {false, premise}, State1);
        _ ->
            %% we successfully saved the motion, the premise is a possibility
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
                                        Mover = get_mover(MotionId),
                                        {Vote, S} = vote_on_motion(Motion, Mover, State1),
                                        vote(MotionId, Vote, S);
                                    _ ->
                                        %% we don't agree about the electorate
                                        vote(MotionId, {nay, electorate}, State1)
                                end;
                            false ->
                                %% not part of the motion's electorate, our vote won't count anyway
                                State1
                        end;
                    _ ->
                        %% its a fiat, no need to vote
                        State1
                end,
            handle_ballot(Motion, Node, State2)
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

maybe_save_motion(Motion = #{deps := ConfId}, MotionId, State = #{elect := Elect}) ->
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
        {_, _, {ratified, _}} ->
            %% the motion is already ratified (must be a conf change, as its still here)
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
        MotionInfo = {_, #{deps := ConfId}, _} ->
            %% the motion is still a possibility (it exists, so its open)
            case util:get(Elect, ConfId) of
                {_, _, {ratified, _}} ->
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

reckon_elections(_, {_, _, {ratified, _}}, State) ->
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
                %% the motion was a conf change, update the last known conf and ratify it
                %% if the current belief is not a descendant of known, change it
                %% any conf siblings must be killed, there can only be one
                %% the peer group should be updated to support the electorate
                %% also, we must emit a message with deps on the ratifying votes (if any)
                %% this keeps the guarantee that subsets of our log determine the peer group for writing
                %% our Point is at least past the ratifying votes, so that will do
                MotionInfo = {Kids, Motion, {ratified, undefined}},
                Elect1 = Elect#{known => MotionId, MotionId => MotionInfo},
                Elect2 =
                    case is_descendant(util:get(Elect1, current), MotionId, Elect1) of
                        true ->
                            Elect1;
                        false ->
                            Elect1#{current => MotionId}
                    end,
                S1 = State#{elect => Elect2},
                S2 = murder_conf_siblings(MotionId, S1),
                S3 = update_conf_peers(MotionId, MotionInfo, S2),
                loom:emit_after_point(#{name => {conf, MotionId}}, S3);
            _ ->
                %% the motion was not a conf change, no need to keep it around
                delete_motion(MotionId, Motion, State)
        end,
    State2 = motion_decided(MotionId, Motion, Mover, Decision, State1),
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
    State3 = motion_decided(MotionId, Motion, Mover, Decision, State2),
    lists:foldl(fun (Kid, S = #{elect := E}) ->
                        reflect_decision(Kid, util:get(E, Kid), {false, premise}, S)
                end, State3, Kids).

vote_on_motion(#{kind := conf}, _, State) ->
    {{yea, ok}, State};
vote_on_motion(#{kind := chain, path := Path, prior := Prior}, _, State) ->
    case util:lookup(State, Path, {undefined, undefined}) of
        {_, _, locked} ->
            %% the path is locked, cannot accept
            {{nay, locked}, State};
        {Value, Prior} ->
            %% the prior matches, accept and lock
            State1 = util:modify(State, Path, {Value, Prior, locked}),
            {{yea, ok}, State1};
        {_, _} ->
            %% the prior doesn't match, cannot accept
            {{nay, version}, State}
    end;
vote_on_motion(Motion, Mover, State) ->
    loom:vote_on_motion(Motion, Mover, State).

motion_decided(MotionId, #{kind := chain, path := Path} = Motion, Mover, Decision, State) ->
    %% NB: motion id must be passed in, do the callbacks need it too? get_motion_id wont work here
    %%     in general from motion_decided we should use the motion id to emit, etc.
    State1 =
        case Decision of
            {true, _} ->
                %% passed a motion to chain: store value and bump version
                Value = util:get(Motion, value),
                Version = MotionId,
                util:modify(State, Path, {Value, Version});
            _ ->
                %% failed to chain: unlock
                util:modify(State, Path, fun ({Value, Version}) ->
                                                 {Value, Version};
                                             ({Value, Version, locked}) ->
                                                 {Value, Version}
                                         end)
        end,
    loom:motion_decided(Motion, Mover, Decision, State1);
motion_decided(_MotionId, Motion, Mover, Decision, State) ->
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

update_conf_peers(ConfId, {Kids, Conf = #{conf := Change}, {ratified, undefined}}, State) ->
    %% change the peer group, and start / stop nodes that are added / removed
    %% initiate a sync right away to cut down on chance that deps will be missing
    %% NB: implementation assumes fully connected peer group and a single electorate
    %%     its possible, but complicated, to support multiple electorates and asymmetric peers
    State1 = loom:change_peers(Change, State),
    Old = maps:keys(util:get(State, peers)),
    New = maps:keys(util:get(State1, peers)),
    State2 = util:modify(State1, [elect, ConfId], {Kids, Conf, {ratified, {New -- Old, Old -- New}}}),
    State3 = erloom_sync:maybe_push(State2),
    case get_mover(ConfId) of
        Node when Node =:= node() ->
            %% only the node that makes the motion to change will try to start / stop the nodes
            %% this is a single point of failure for changing the group, currently
            %% in theory other nodes could try to exert control if they think something is wrong
            loom:create_task({conf, ConfId}, {fun control_conf_peers/2, ConfId}, State3);
        _ ->
            State3
    end.

control_conf_peers(ConfId, State = #{spec := Spec}) ->
    %% the first try may reach the targets before the deps and fail
    %% its fine, we'll just retry, these messages are relatively uncommon anyway
    case ready_to_control(ConfId, State) of
        {true, Pending} ->
            case Pending of
                {[], []} ->
                    {done, ok};
                {Start, Stop} ->
                    %% give the sync a little bit of a chance to work by waiting a sec
                    receive after 1000 -> ok end,
                    _ = loom:multicall(Start, [Spec, #{deps => ConfId, type => start}]),
                    _ = loom:multicall(Stop, [Spec, #{deps => ConfId, type => stop}]),
                    {retry, {15, seconds}}
            end;
        {false, not_ready} ->
            {retry, {60, seconds}};
        {false, Reason} ->
            {error, Reason}
    end.

ready_to_control(ConfId, State) ->
    %% the parent change must have no more pending changes before we are ready to exert control
    %% this guarantees *all* start / stops happen in order
    case util:lookup(State, [elect, ConfId]) of
        {_, Conf = #{deps := _}, {ratified, Pending = {_, _}}} ->
            case get_parent(Conf, State) of
                {_, _, {ratified, {[], []}}} ->
                    {true, Pending};
                {_, _, {ratified, {_, _}}} ->
                    {false, not_ready};
                _ ->
                    {false, bad_parent}
            end;
        {_, _Seed, {ratified, Pending = {_, _}}} ->
            {true, Pending};
        _ ->
            {false, bad_conf}
    end.
