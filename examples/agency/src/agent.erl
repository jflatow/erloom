-module(agent).

-behavior(loom).
-export([home/1,
         opts/1,
         write_through/3,
         handle_message/4,
         vote_on_motion/3,
         motion_decided/4,
         handle_idle/1]).

home({agent, Name}) ->
    filename:join([var, url:esc(node()), Name]).

opts({agent, _}) ->
    #{
      idle_timeout => time:timeout({5, minutes}),
      sync_interval => 3000 + random:uniform(3000),
      sync_push_prob => 0.5,
      unanswered_max => 1
     }.

write_through(#{do := get_state}, _N, _State) ->
    {0, infinity};
write_through(#{type := lookup}, _N, _State) ->
    {0, infinity};
write_through(#{write := W}, _N, _State) when is_integer(W) ->
    {W, 1000};
write_through(_Message, _N, _State) ->
    {1, infinity}.

handle_message(#{do := save}, _Node, true, State) ->
    loom:maybe_reply(loom:save(State));
handle_message(#{do := get_state}, _Node, true, State) ->
    loom:maybe_reply(State, State);
handle_message(#{do := emit}, _Node, true, State) ->
    loom:create_yarn(#{do => reply}, State);
handle_message(#{do := reply}, _Node, true, State) ->
    loom:maybe_reply(got_it, State);
handle_message(#{chain := {Path, Value}}, _Node, true, State) ->
    loom:maybe_chain(#{path => Path, value => Value}, State);
handle_message(#{type := start}, Node, true, State) ->
    io:format("~p started~n", [Node]),
    loom:maybe_reply(State);
handle_message(#{type := stop}, Node, true, State) ->
    io:format("~p stopped~n", [Node]),
    loom:maybe_reply(State);
handle_message(#{type := task, name := Name, kind := done}, Node, true, State) ->
    io:format("~p completed task ~p~n", [Node, Name]),
    loom:maybe_reply(State);
handle_message(#{write := _}, _, true, State = #{wrote := Wrote}) ->
    loom:maybe_reply(Wrote, State);
handle_message(Message, Node, true, State) ->
    io:format("~p new message: ~p~n", [Node, Message]),
    loom:maybe_reply(State);
handle_message(_, _, false, State) ->
    State.

vote_on_motion(_Motion, Mover, State) ->
    io:format("~p vote on ~p from ~p ~n", [node(), erloom:locus_node(State), Mover]),
    {{yea, ok}, State}.

motion_decided(#{kind := chain, path := xyz}, Mover, {true, _}, State) when Mover =:= node() ->
    GenVal = base64url:encode(crypto:rand_bytes(8)),
    Modify = #{type => modify, kind => chain, path => xyz, value => GenVal},
    State1 = loom:suture_yarn(Modify, State),
    loom:maybe_reply([Mover, 'did pass chain'], State1);
motion_decided(#{yarn := Yarn}, Mover, Decision, State) ->
    io:format("~p decided ~p for ~p from ~p~n", [node(), Decision, Yarn, Mover]),
    loom:maybe_reply(Decision, State).

handle_idle(State) ->
    io:format("idling~n"),
    loom:sleep(State).
