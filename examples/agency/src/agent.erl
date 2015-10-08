-module(agent).

-behavior(loom).
-export([home/1,
         opts/1,
         write_through/3,
         handle_idle/1,
         handle_info/2,
         handle_message/4,
         vote_on_motion/3,
         motion_decided/4]).

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

handle_idle(State) ->
    io:format("idling~n"),
    loom:sleep(State).

handle_info(Info, State) ->
    io:format("info: ~p~n", [Info]),
    State.

handle_message(#{do := save}, _Node, true, State) ->
    loom:save(State);
handle_message(#{do := get_state}, _Node, true, State) ->
    State#{response => State};
handle_message(#{do := emit}, _Node, true, State) ->
    loom:stitch_yarn(#{do => reply}, State);
handle_message(#{do := reply}, _Node, true, State) ->
    State#{response => got_it};
handle_message(#{type := start}, Node, true, State) ->
    io:format("~p started~n", [Node]),
    State;
handle_message(#{type := stop}, Node, true, State) ->
    io:format("~p stopped~n", [Node]),
    State;
handle_message(#{type := task, name := Name, kind := done}, Node, true, State) ->
    io:format("~p completed task ~p~n", [Node, Name]),
    State;
handle_message(#{write := _}, _, true, State = #{wrote := Wrote}) ->
    State#{response => Wrote};
handle_message(Message, Node, true, State) ->
    io:format("~p new message: ~p~n", [Node, Message]),
    State;
handle_message(_, _, false, State) ->
    State.

vote_on_motion(Motion, Mover, State) ->
    io:format("~p vote on ~p from ~p ~n", [node(), Motion, Mover]),
    {{yea, ok}, State}.

motion_decided(#{kind := chain, path := xyz}, Mover, {true, _}, State) when Mover =:= node() ->
    GenVal = base64url:encode(crypto:rand_bytes(8)),
    Modify = #{type => command, verb => modify, kind => chain, path => xyz, value => GenVal},
    State1 = loom:suture_yarn(Modify, State),
    State1#{response => [Mover, 'did pass chain']};
motion_decided(#{yarn := Yarn}, Mover, Decision, State) ->
    io:format("~p decided ~p for ~p from ~p~n", [node(), Decision, Yarn, Mover]),
    State.
