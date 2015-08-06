-module(erloom_sync).

-export([do_push/2,
         maybe_push/1,
         maybe_pull/2,
         got_sync/2,
         send_sync/3,
         trap_sync/2]).

do_push(Entries, State = #{peers := Peers}) ->
    %% definitely push entries to all peers (i.e. for writing)
    maps:fold(fun (Node, _, S) ->
                      send_sync(Node, #{entries => Entries}, S)
              end, State, Peers).

maybe_push(State = #{edges := Edges, front := Front, peers := Peers}) ->
    %% send sync to any peers we are ahead of, or not at all
    %% just include our front, no requests, no entries
    %% how many nodes are in the cluster? should we just count #edges + 1?
    %% just because we have logs from a node, doesn't mean its still around and/or counts
    %% just because we have never heard from a node, doesn't mean it doesn't count
    %% if e.g. we want to write a quorum, we need to actually know the 'right' answer
    %% hence, we have peers, which are permanently stored in the state
    maps:fold(fun (Node, _, S) ->
                      Edge = util:get(Edges, Node, #{}),
                      case erloom:edge_delta(Front, Edge) of
                          Delta when map_size(Delta) > 0 ->
                              send_sync(Node, #{}, S);
                          _ ->
                              S
                      end
              end, State, Peers).

maybe_pull(Edge, State = #{front := Front}) ->
    %% whereever our front is behind the edge, request the rest of the log from someone else
    %% we could randomly select a node that has what we need, but for now we always ask the owner
    maps:fold(fun (Node, {Mark, _}, S) ->
                      send_sync(Node, #{requests => #{Node => {Mark, undefined}}}, S)
              end, State, erloom:edge_delta(Edge, Front)).

got_sync(Packet = #{from := {FromNode, FromPid}, front := Edge}, State) ->
    %% one node's front is another node's edge
    State1 = util:modify(State, [edges, FromNode],
                         fun (PrevEdge) ->
                                 erloom:edge_hull(Edge, util:def(PrevEdge, #{}))
                         end),
    State2 = util:modify(State1, [cache, FromNode], {FromPid, 0}),
    handle_sync(Packet, State2).

handle_sync(Packet = #{from := {FromNode, _}, entries := Entries}, State) ->
    %% if the entries match & catch up the logs they are part of, just write them & ack
    %% otherwise keep requesting until there are no gaps and we reach the edge
    %% if replicas diverge we are screwed, but it should be impossible given our invariants
    {Reply, State1} =
        maps:fold(fun (Node, [{{Before, _After}, _}|_] = EntryList, {R, S}) ->
                          {Log, S1} = loom:obtain_log(Node, S),
                          case log:locus(Log) of
                              Tip when Tip =:= Before ->
                                  %% the logs match, just write the entries
                                  {R, loom:extend_log(Log, EntryList, Node, S1)};
                              Tip when Tip < Before ->
                                  %% theres a gap (or apocalypse): request to fill ourselves in
                                  Mark = util:lookup(S1, [edges, FromNode, Node]),
                                  R1 = util:modify(R, [requests, Node], {Tip, Mark}),
                                  {R1, S1};
                              Tip when Tip > Before ->
                                  %% we are ahead (or apocalypse): ignore and assume we are getting the data elsewhere
                                  {R, S1}
                          end;
                      (_, [], {R, S}) ->
                          {R, S}
                  end, {#{}, State}, Entries),
    reply_sync(Packet, Reply, State1);
handle_sync(Packet = #{requests := Requests}, State = #{opts := Opts}) ->
    %% reply with whatever entries they requested (or at least an initial subset up to limit size)
    RangeOpts = #{limit => util:get(Opts, sync_log_limit)},
    {Reply, State1} =
        maps:fold(fun (Node, Range, {R, S}) ->
                          %% we include entries for every request, even if its empty
                          %% this ensures we are sent a reply under normal circumstances
                          {Log, S1} = loom:obtain_log(Node, S),
                          EntryList = log:range(Log, Range, RangeOpts),
                          R1 = util:modify(R, [entries, Node], lists:reverse(EntryList)),
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
    Packet1 = Packet#{from => loom:delegate(State), front => Front},
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
