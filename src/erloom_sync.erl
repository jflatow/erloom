-module(erloom_sync).
-author("Jared Flatow").

-export([do_push/2,
         maybe_push/1,
         maybe_pull/2,
         got_sync/2,
         send_sync/3,
         trap_sync/2]).

delegate(#{listener := Listener}) ->
    {node(), Listener}.

do_push(Entries, State = #{peers := Peers}) ->
    %% definitely push entries to all peers (i.e. for writing)
    maps:fold(fun (Node, _, S) ->
                      send_sync(Node, #{entries => Entries}, S)
              end, State, Peers).

maybe_push(State = #{status := Status}) when Status =/= awake ->
    %% do not push if we are not awake, otherwise we could push to very old peers
    %% if we push to old peers its fine except we might never reclaim the disk space
    %% NB: so recovering nodes can't fill each other in until they completely recover
    %%     if *all* nodes are recovering they can never fully recover (automatically)
    %%     even if together they have a complete set of logs
    State;
maybe_push(State = #{edges := Edges, front := Front, peers := Peers, opts := Opts}) ->
    %% send sync to any peers we think we are ahead of, or not at all
    %% we also randomly sync regardless, in case the node got replaced
    %% just include our front, no requests, no entries
    SyncPushProb = util:get(Opts, sync_push_prob),
    PushAnyway = random:uniform() < SyncPushProb,
    maps:fold(fun (Node, _, S) ->
                      Edge = util:get(Edges, Node, #{}),
                      case erloom:edge_delta(Front, Edge) of
                          Delta when map_size(Delta) > 0 ->
                              send_sync(Node, #{}, S);
                          _ when PushAnyway ->
                              send_sync(Node, #{}, S);
                          _ ->
                              S
                      end
              end, State, Peers).

maybe_pull(Missing, State = #{edges := Edges, front := Front}) ->
    %% whereever our front is behind the missing edge, request the rest of the log from someone else
    %% we could randomly select a node that has what we need, or always ask the owner
    %% but its possible that we are the owner of a log and we don't have it (if we are reincarnated)
    %% we request it from whoever we think is furthest ahead (its ok if we are wrong or can't get it)
    maps:fold(fun (Node, {Mark, _}, S) ->
                      case erloom:edges_max(Node, Edges) of
                          {undefined, undefined} ->
                              S;
                          {_, Max} when Max < Mark ->
                              S;
                          {MaxNode, Max} ->
                              send_sync(MaxNode, #{requests => #{Node => {Mark, Max}}}, S)
                      end
              end, State, erloom:edge_delta(Missing, Front)).

got_sync(Packet = #{from := {FromNode, FromPid}, front := Edge}, State) ->
    %% one node's front is another node's edge
    %% the front should never decrease, except if a node resets
    %% packets may not arrive in order, but for the most part they do
    %% we might advertise some extra pushes to a node because its fronts come out of order
    %% but if we take the edge hull, we wont realize when a node is reset
    State1 = util:modify(State, [edges, FromNode], Edge),
    State2 = util:modify(State1, [cache, FromNode], {FromPid, 0}),
    State3 =
        case State2 of
            S = #{status := waiting} ->
                case util:get(Edge, node()) of
                    undefined ->
                        S;
                    _ ->
                        %% the node knows something about our history
                        %% we must have had a prior life, time to recover
                        S#{status => recovering}
                end;
            S ->
                S
        end,
    handle_sync(Packet, State3).

handle_sync(Packet = #{from := {FromNode, _}, entries := Entries}, State) ->
    %% if the entries match & catch up the logs they are part of, just write them & ack
    %% otherwise keep requesting until there are no gaps and we reach the edge
    %% if replicas diverge we are screwed, but it should be impossible given our invariants
    {Reply, State1} =
        maps:fold(fun (Which = {Node, IId}, [{{Before, _After}, _}|_] = EntryList, {R, S}) ->
                          {Log, S1} = erloom_logs:obtain(Which, S),
                          case log:locus(Log) of
                              Pos when Pos =:= Before ->
                                  %% the logs match, just append the entries
                                  {R, erloom_logs:extend(Log, EntryList, Which, S1)};
                              Pos when Pos < Before ->
                                  %% theres a gap (or apocalypse): request to fill ourselves in
                                  Mark = util:lookup(S1, [edges, FromNode, Node]),
                                  R1 = util:modify(R, [requests, Node], {{IId, Pos}, Mark}),
                                  {R1, S1};
                              Pos when Pos > Before ->
                                  %% we are ahead (or apocalypse): ignore and assume we are getting the data elsewhere
                                  {R, S1}
                          end;
                      (_, [], {R, S}) ->
                          {R, S}
                  end, {#{}, State}, Entries),
    reply_sync(Packet, Reply, State1);
handle_sync(Packet = #{requests := Requests}, State = #{opts := Opts}) ->
    %% reply with whatever entries they requested (or at least an initial subset up to limit size)
    LogLimit = util:get(Opts, sync_log_limit),
    {Reply, State1} =
        maps:fold(fun (Node, Range, {R, S}) ->
                          %% we include entries for every request, even if its empty
                          %% this ensures we are sent a reply under normal circumstances
                          {Which, EntryList, S1} = erloom_logs:slice(LogLimit, Range, Node, S),
                          R1 = util:modify(R, [entries, Which], lists:reverse(EntryList)),
                          {R1, S1}
                  end, {#{}, State}, Requests),
    reply_sync(Packet, Reply, State1);
handle_sync(Packet = #{sequence := 0, front := Edge}, State) ->
    %% first message has no entries or requests: its an offer to push
    %% reply with what we want them to push (or nothing, i.e. ack)
    {Reply, State1} =
        maps:fold(fun (Node, Mark, {R, S}) ->
                          %% our tip could be undefined, which is handled correctly (< Mark)
                          %% a pattern match on the Front would require an extra clause
                          case util:lookup(S, [front, Node]) of
                              Tip when Tip < Mark ->
                                  R1 = util:modify(R, [requests, Node], {Tip, Mark}),
                                  {R1, S};
                              _ ->
                                  {R, S}
                          end
                  end, {#{}, State}, Edge),
    reply_sync(Packet, Reply, State1);
handle_sync(_, State) ->
    %% message has no entries or requests: its an ack, we are done syncing
    State.

send_sync(Node, Packet = #{sequence := N}, State = #{front := Front, opts := Opts}) ->
    %% we try to cache the pid of our counterpart on Node
    %% we count how many times we sent the node a packet, since we last heard from it
    %% but only counting those packets for which we expect a reply
    %% when we receive a packet from Node, we reset the pid and counter
    %% thus the counter should be 0 (cache is warm) whenever sequence number is > 0
    %% if our counter goes over an arbitrary limit, there are too many pending packets
    %% the limit implies we should have heard back at least once after sending that many packets
    %% thus if we hit the limit, and we have a pid, we assume the pid is no longer valid
    %% if we don't have a pid, we consider that there might be something wrong with the node
    UnansweredMax = util:get(Opts, unanswered_max),
    Increment = 1 - N rem 2, %% only even numbered packets expect a reply
    Packet1 = Packet#{from => delegate(State), front => Front},
    State1 =
        case util:lookup(State, [cache, Node], {undefined, 0}) of
            {undefined, Unanswered} when Unanswered + Increment > UnansweredMax ->
                %% if we don't have a pid and go over the limit, the node should be checked
                U = Unanswered + Increment,
                S = loom:check_node(Node, U, State),
                util:modify(S, [cache, Node], {undefined, U});
            {Pid, Unanswered} when is_pid(Pid), Unanswered + Increment > UnansweredMax ->
                %% if we have a pid and go over the limit, we forget the pid and start over
                util:modify(State, [cache, Node], {undefined, Increment});
            {Either, Unanswered} ->
                %% otherwise just increment the counter
                util:modify(State, [cache, Node], {Either, Unanswered + Increment})
        end,
    cast_sync(Node, Packet1, State1);
send_sync(Node, Packet, State) ->
    send_sync(Node, Packet#{sequence => 0}, State).

cast_sync(Node, Packet, State = #{spec := Spec}) ->
    case util:lookup(State, [cache, Node], {undefined, 0}) of
        {undefined, _} ->
            %% we dont have a pid, so we cast to Node, passing the Spec
            true = rpc:cast(Node, ?MODULE, trap_sync, [Spec, Packet]),
            State;
        {Pid, _} when is_pid(Pid) ->
            %% we have a pid for the node, we try to contact it directly
            Pid ! {sync_logs, Packet},
            State
    end.

trap_sync(Spec, Packet) ->
    %% another node needs us: bootstrap using the Spec, then deliver the Packet
    Pid = loom:proc(Spec),
    Pid ! {sync_logs, Packet}.

reply_sync(#{from := {FromNode, _}, sequence := N}, Reply, State) ->
    send_sync(FromNode, Reply#{sequence => N + 1}, State).
