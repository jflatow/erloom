-module(erloom_logs).

-export([load/1,
         path/2,
         empty/2,
         obtain/2,
         extend/4,
         replay/4,
         slice/4,
         write/2]).

load(State = #{ours := _, logs := _, front := _}) ->
    lists:foldl(fun (NodePath, S) ->
                        Node = util:atom(url:unescape(filename:basename(NodePath))),
                        IIds = [util:bin(I) || I <- lists:reverse(path:ls(NodePath))],
                        load_node(Node, IIds, S)
                end, State, path:list(loom:path(logs, State)));
load(State) ->
    load(load_ours(State#{logs => #{}, front => #{}})).

load_node(Node, [IId|Rest], State = #{front := Front}) ->
    case empty({Node, IId}, State) of
        {true, _Log, State1} ->
            %% dont load or use the front from empty logs
            %% this *should* only happen if we are recovering:
            %%  in that case we must ignore the new log front until we write to it
            %% it *can* also happen if we die in the middle of a sync:
            %%  in that case we will open the new log later
            load_node(Node, Rest, State1);
        {false, Log, State1} ->
            %% if the log has entries we're good, update the front accordingly
            State1#{front => Front#{Node => {IId, log:locus(Log)}}}
    end;
load_node(_, [], State) ->
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
            %% we must wait to recover or be seeded before we can write to our log
            State1#{status => waiting};
        {false, _Log, State1} ->
            %% we wrote, we wouldn't have previously written unless we were ready before
            %% as soon as we get to the front we will be ready again
            State1#{status => ok}
    end.

path({Node, IId}, State) ->
    loom:path([logs, url:esc(Node), IId], State);
path(Node, State) ->
    loom:path([logs, url:esc(Node)], State).

first_id(Node, State) ->
    case path:ls(path(Node, State)) of
        [] ->
            throw({no_logs, Node});
        [Name|_] ->
            util:bin(Name)
    end.

next_id(Which, State) ->
    case path:next(path(Which, State)) of
        undefined ->
            throw({no_next_log, Which});
        Path ->
            util:bin(filename:basename(Path))
    end.

empty(Which, State) ->
    {Log, State1} = obtain(Which, State),
    {log:first(Log) =:= undefined, Log, State1}.

%% get a handle for a log, whether we have it yet or not

obtain(Which, State = #{logs := Logs}) ->
    case util:get(Logs, Which) of
        undefined ->
            {ok, Log} = log:open(path(Which, State)),
            {Log, State#{logs => Logs#{Which => Log}}};
        Log ->
            {Log, State}
    end.

%% fold through a range, accumulating into state

replay(Fun, {undefined, {BId, B}}, Node, State) ->
    replay(Fun, {{first_id(Node, State), undefined}, {BId, B}}, Node, State);
replay(Fun, {{AId, A}, {AId, B}}, Node, State) ->
    {Log, State1} = obtain({Node, AId}, State),
    {{_, After}, State2 = #{point := Point}} =
        log:bendl(Log,
                  fun ({{Before, _}, Data}, S = #{point := P}) ->
                          %% NB: we mark Before in case Fun decides to exit early
                          Fun(binary_to_term(Data), Node, S#{point => P#{Node => {AId, Before}}})
                  end, State1, {A, B}),
    State2#{point => Point#{Node => {AId, After}}};
replay(Fun, {{AId, A}, {BId, B}}, Node, State) when AId < BId ->
    IId = next_id({Node, AId}, State),
    State1 = replay(Fun, {{AId, A}, {AId, undefined}}, Node, State),
    replay(Fun, {{IId, undefined}, {BId, B}}, Node, State1).

%% grab the next batch of log entries in the range (i.e. for syncing)

slice(Limit, {undefined, {BId, B}}, Node, State) ->
    slice(Limit, {{first_id(Node, State), undefined}, {BId, B}}, Node, State);
slice(Limit, {{AId, A}, {AId, B}}, Node, State) ->
    {Log, State1} = obtain({Node, AId}, State),
    EntryList = log:range(Log, {A, B}, #{limit => Limit}),
    {{Node, AId}, EntryList, State1};
slice(Limit, {{AId, A}, {BId, B}}, Node, State) when AId < BId ->
    {Log, State1} = obtain({Node, AId}, State),
    case log:range(Log, {A, undefined}, #{limit => Limit}) of
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
    {Log, State1} = obtain(Ours, State),
    {ok, {_, After} = Range} = log:write(Log, Data),
    {[{Range, Data}], State1#{front => Front#{Node => {IId, After}}}}.
