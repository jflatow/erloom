-module(erloom_logs).
-author("Jared Flatow").

-export([load/1,
         path/2,
         empty/2,
         close/1,
         obtain/2,
         fold/3,
         replay/3,
         replay/4,
         slice/4,
         extend/4,
         write/2]).

ls(Path, forward) ->
    path:ls(Path);
ls(Path, reverse) ->
    lists:reverse(ls(Path, forward)).

load(State) ->
    load_ours(load_logs(State)).

load_logs(State) ->
    load_logs(State, forward).

load_logs(State, Direction) ->
    load_logs(path:list(loom:path(logs, State)), State#{logs => #{}, front => #{}}, Direction).

load_logs(NodePaths, State, Direction) ->
    lists:foldl(fun (P, S) -> load_node(P, S, Direction) end, State, NodePaths).

load_node(NodePath, State, Direction) ->
    Node = util:atom(url:unescape(filename:basename(NodePath))),
    IIds = [util:bin(I) || I <- lists:reverse(ls(NodePath, Direction))],
    load_node(Node, IIds, State, Direction).

load_node(Node, [IId|Rest], State = #{front := Front}, Direction) ->
    case empty({Node, IId}, State) of
        {true, _Log, State1} ->
            %% dont load or use the front from empty logs
            %% this *should* only happen if we are recovering:
            %%  in that case we must ignore the new log front until we write to it
            %% it *can* also happen if we die in the middle of a sync:
            %%  in that case we will open the new log later
            load_node(Node, Rest, State1, Direction);
        {false, Log, State1} when Direction =:= forward ->
            %% if the log has entries we're good, update the front accordingly
            State1#{front => Front#{Node => {IId, log:locus(Log)}}};
        {false, Log, State1} when Direction =:= reverse ->
            %% if we're going in reverse, use the first position in the log
            State1#{front => Front#{Node => {IId, log:zilch(Log)}}}
    end;
load_node(_, [], State, _) ->
    State.

load_ours(State) ->
    %% the 'ours' link tells us if we've reset or not: if it's missing, we should start a new log
    %% otherwise, in a previous incarnation we may have had a log for which we are missing entries
    %% in that case, others could be ahead of us on our own log, which would be a disaster
    %% NB: restarting from a backup image can be apocalyptic if it preserves the link!
    %%     if replicas are not enough for some reason, just remove the links from snapshots
    OursPath = loom:path(ours, State),
    Ours =
        case file:read_link(OursPath) of
            {ok, Name} ->
                %% if we have the link, it tells us which log is ours
                ["logs", NodePath, IId] = filename:split(Name),
                {util:atom(url:unescape(NodePath)), util:bin(IId)};
            {error, enoent} ->
                %% if not, create a new log
                %% we rely on the clock of a replacement node returning a later second than the start time of the node it replaces
                %% in general its a bad idea to rely on clocks, but this particular constraint is not too difficult to maintain
                %% node replacements generally happen on the scale of months and years, clocks are reliable enough at that resolution
                %% this allows us to order logs of the same node, and keep the size of the front O(nodes) instead of O(incarnations)
                IId = time:stamp(time:unow(), tai64),
                ok = path:link(filename:join(["logs", url:esc(node()), IId]), OursPath),
                {node(), IId}
        end,
    case empty(Ours, State#{ours => Ours}) of
        {true, _Log, State1} ->
            %% we never wrote, maybe we died trying, maybe we were never started
            %% we must wait to recover or be started before we can write to our log
            State1#{status => waiting};
        {false, _Log, State1} ->
            %% we wrote, we wouldn't have previously written unless we were ready before
            %% as soon as we get to the front we will be ready again
            State1#{status => waking}
    end.

path({Node, IId}, State) ->
    loom:path([logs, url:esc(Node), IId], State);
path(Node, State) ->
    loom:path([logs, url:esc(Node)], State).

first_id(Node, State) ->
    first_id(Node, State, forward).

first_id(Node, State, Direction) ->
    case ls(path(Node, State), Direction) of
        [] ->
            throw({no_logs, Node});
        [Name|_] ->
            util:bin(Name)
    end.

next_id(Which, State) ->
    next_id(Which, State, forward).

next_id(Which, State, Direction) ->
    case path:next(path(Which, State), Direction) of
        undefined ->
            throw({no_next_log, Which});
        Path ->
            util:bin(filename:basename(Path))
    end.

empty(Which, State) ->
    {Log, State1} = obtain(Which, State),
    {log:first(Log) =:= undefined, Log, State1}.

close(State = #{logs := Logs}) ->
    maps:fold(fun (_, Log, _) -> log:close(Log) end, nil, Logs),
    util:delete(State, logs).

%% get a handle for a log, whether we have it yet or not

obtain(Which, State = #{logs := Logs}) ->
    case util:get(Logs, Which) of
        undefined ->
            {ok, Log} = log:open(path(Which, State)),
            {Log, State#{logs => Logs#{Which => Log}}};
        Log ->
            {Log, State}
    end.

%% fold over logs, as needed by the outside world (not used internally by loom)
%% can perform fold given either state or home directory, and can limit num folds

fold(Fun, Acc, #{
            logs := _,
            front := Front,
            range := {Start, Stop},
            direction := Direction
           } = State) ->
    try
        {Point, Target} =
            case Direction of
                forward ->
                    {util:def(Start, #{}), util:def(Stop, Front)};
                reverse ->
                    {util:def(Stop, #{}), util:def(Start, Front)}
            end,
        State1 = State#{acc => Acc, point => Point},
        State2 = replay(fun (Message, _Node, S = #{acc := A, locus := L}) ->
                                case util:get(S, limit) of
                                    undefined ->
                                        S#{acc => Fun({L, Message}, A)};
                                    0 ->
                                        throw({stop, S});
                                    N ->
                                        S#{acc => Fun({L, Message}, A), limit => N - 1}
                                end
                        end, [Target], State1, Direction),
        util:get(State2, acc)
    catch
        %% fold only as far as we can go
        throw:{unreachable, Tgt, #{acc := A}} ->
            throw({unreachable, Tgt, A});
        throw:{unsupported, Vsn, #{acc := A}} ->
            throw({unsupported, Vsn, A});
        throw:{stop, #{acc := A}} ->
            A
    end;
fold(Fun, Acc, #{home := _, range := _, direction := Direction} = Opts) ->
    fold(Fun, Acc, load_logs(Opts, Direction));
fold(Fun, Acc, #{home := _, range := _} = Opts) ->
    fold(Fun, Acc, Opts#{direction => forward});
fold(Fun, Acc, #{home := _} = Opts) ->
    fold(Fun, Acc, Opts#{range => {undefined, undefined}});
fold(Fun, Acc, Home) ->
    fold(Fun, Acc, #{home => Home}).

%% replay all logs, ensuring each target one at a time or failing

replay(Fun, Targets, State) ->
    replay(Fun, Targets, State, forward).

replay(Fun, [Target|Stack], State = #{point := Point, front := Front}, Direction) ->
    Replay =
        fun (Message, Node, S) ->
                case loom:unmet_needs(Message, S, Direction) of
                    nil ->
                        Fun(Message, Node, S);
                    {vsn, Vsn} ->
                        throw({unsupported, Vsn, S});
                    {deps, Deps} when Direction =:= forward ->
                        replay(Fun, [Deps, Target], S, Direction);
                    {deps, Deps} when Direction =:= reverse ->
                        replay(Fun, [Deps, Target], Fun(Message, Node, S), Direction)
                end
        end,
    case erloom:edge_delta(Target, Point, Direction) of
        TP when map_size(TP) > 0 ->
            %% target is ahead of point: try to reach it
            case erloom:edge_delta(Target, Front, Direction) of
                TF when map_size(TF) > 0 ->
                    %% target is unreachable: stop
                    throw({unreachable, Target, State});
                _ ->
                    %% target is contained in front
                    %% go after a node that's ahead, then try the whole thing again
                    %% since we may have covered more (or less) ground than we intended
                    [{Node, Range}|_] = maps:to_list(TP),
                    State1 = replay(Replay, Range, Node, State, Direction),
                    replay(Fun, [Target|Stack], State1, Direction)
            end;
        _ ->
            %% target is reached
            replay(Fun, Stack, State, Direction)
    end;
replay(_, [], State, _) ->
    State.

%% replay a single node range, accumulating into state

replay(Fun, {undefined, {BId, B}}, Node, State, Direction) ->
    replay(Fun, {{first_id(Node, State, Direction), undefined}, {BId, B}}, Node, State, Direction);
replay(Fun, {{AId, A}, undefined}, Node, State, Direction) ->
    replay(Fun, {{AId, A}, {first_id(Node, State, Direction), undefined}}, Node, State, Direction);
replay(Fun, {{AId, A}, {AId, B}}, Node, State, forward) ->
    {Log, State1} = obtain({Node, AId}, State),
    {{_, After}, State2 = #{point := Point}} =
        log:bendl(Log,
                  fun ({{At, _}, _}, S = #{point := #{Node := Mark}}) when {AId, At} < Mark ->
                          %% point already got ahead of us, just skip
                          %% i.e. we already processed this data during another target
                          S;
                      ({{At, _} = Range, {ok, Data}}, S = #{point := P}) ->
                          %% NB: we mark At in case Fun decides to exit early
                          %% i.e. the state always reflects the point accurately
                          P1 = P#{Node => {AId, At}},
                          S1 = S#{point => P1, locus => {{Node, AId}, Range}},
                          Fun(binary_to_term(Data), Node, S1);
                      ({_, {nil, _}}, S) ->
                          %% skip nullified data (not safe, but can be done manually)
                          S
                  end, State1, {A, B}, #{rash => true}),
    State2#{point => Point#{Node => {AId, After}}};
replay(Fun, {{AId, A}, {AId, B}}, Node, State, reverse) ->
    {Log, State1} = obtain({Node, AId}, State),
    {{After, _}, State2 = #{point := Point}} =
        log:bendr(Log,
                  fun ({{At, _}, _}, S = #{point := #{Node := Mark}}) when {AId, At} >= Mark ->
                          %% point already got ahead of us, just skip
                          %% i.e. we already processed this data during another target
                          S;
                      ({{At, _} = Range, {ok, Data}}, S = #{point := P}) ->
                          %% NB: we mark At in case Fun decides to exit early
                          %% i.e. the state always reflects the point accurately
                          P1 = P#{Node => {AId, At}},
                          S1 = S#{point => P1, locus => {{Node, AId}, Range}},
                          Fun(binary_to_term(Data), Node, S1);
                      ({_, {nil, _}}, S) ->
                          %% skip nullified data (not safe, but can be done manually)
                          S
                  end, State1, {A, B}, #{rash => true}),
    State2#{point => Point#{Node => {AId, After}}};
replay(Fun, {{AId, A}, {BId, B}}, Node, State, Direction) when AId < BId ->
    IId = next_id({Node, AId}, State, Direction),
    State1 = replay(Fun, {{AId, A}, {AId, undefined}}, Node, State, Direction),
    replay(Fun, {{IId, undefined}, {BId, B}}, Node, State1, Direction).

%% grab the next batch of log entries in the range (i.e. for syncing)

slice(Limit, {undefined, {BId, B}}, Node, State) ->
    slice(Limit, {{first_id(Node, State), undefined}, {BId, B}}, Node, State);
slice(Limit, {{AId, A}, {AId, B}}, Node, State) ->
    {Log, State1} = obtain({Node, AId}, State),
    EntryList = log:range(Log, {A, B}, #{limit => Limit, rash => true}),
    {{Node, AId}, EntryList, State1};
slice(Limit, {{AId, A}, {BId, B}}, Node, State) when AId < BId ->
    {Log, State1} = obtain({Node, AId}, State),
    case log:range(Log, {A, undefined}, #{limit => Limit, rash => true}) of
        [] ->
            IId = next_id({Node, AId}, State1),
            slice(Limit, {{IId, undefined}, {BId, B}}, Node, State1);
        EntryList ->
            {{Node, AId}, EntryList, State1}
    end.

%% we extend other logs (e.g. of our peers)

extend(Log, [Entry|Rest], Which = {Node, IId}, State = #{front := Front}) ->
    {{_, After} = Range, Data} = Entry,
    {ok, Range} = log:write(Log, Data),
    extend(Log, Rest, Which, State#{front => Front#{Node => {IId, After}}});
extend(_, [], _, State) ->
    State.

%% we only write our own log

write(Message, State) when is_map(Message) ->
    write(term_to_binary(Message), State);
write(Data, State = #{ours := Ours = {Node, IId}, front := Front}) ->
    %% we assume we are caught up, otherwise we shouldn't be writing
    %% thus locus needs to span the new entry
    {Log, State1} = obtain(Ours, State),
    {ok, {_, After} = Range} = log:write(Log, Data),
    {[{Range, Data}],
     State1#{front => Front#{Node => {IId, After}},
             locus => {Ours, Range}}}.
