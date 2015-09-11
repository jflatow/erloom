-module(agent).

-behavior(loom).
-export([home/1,
         opts/1,
         write_through/3,
         handle_message/3,
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
write_through(#{write := W}, _N, _State) when is_integer(W) ->
    {W, 1000};
write_through(_Message, _N, _State) ->
    {1, infinity}.

handle_message(#{do := save}, _Node, State) ->
    loom:maybe_reply(ok, loom:save(State));
handle_message(#{do := get_state}, _Node, State) ->
    loom:maybe_reply(State, State);
handle_message(#{do := emit} = Message, _Node, State) ->
    case loom:is_incoming(Message, State) of
        true ->
            loom:emit_message(#{do => reply}, carry, State);
        false ->
            loom:maybe_reply(never_reply, State)
    end;
handle_message(#{do := reply}, _Node, State) ->
    loom:maybe_reply(got_it, State);
handle_message(#{chain := {Path, Value}} = Message, _Node, State) ->
    case loom:is_incoming(Message, State) of
        true ->
            loom:defer_reply(loom:chain_value(Path, Value, State));
        false ->
            State
    end;
handle_message(#{type := start}, Node, State) ->
    io:format("~p started~n", [Node]),
    loom:maybe_reply(ok, State);
handle_message(#{type := stop}, Node, State) ->
    io:format("~p stopped~n", [Node]),
    loom:maybe_reply(ok, State);
handle_message(#{type := move}, Node, State) ->
    io:format("~p moved, deferring reply~n", [Node]),
    loom:defer_reply(State);
handle_message(#{type := task, name := Name, done := _}, Node, State) ->
    io:format("~p completed task ~p~n", [Node, Name]),
    loom:maybe_reply(ok, State);
handle_message(#{write := _}, _Node, State = #{wrote := Wrote}) ->
    loom:maybe_reply(Wrote, State);
handle_message(Message, Node, State) ->
    io:format("~p message: ~p~n", [Node, Message]),
    loom:maybe_reply(ok, State).

vote_on_motion(#{name := Name}, Mover, State) ->
    io:format("~p vote on ~p from ~p ~n", [node(), Name, Mover]),
    {{yea, ok}, State}.

motion_decided(#{kind := chain, path := xyz} = Motion, Mover, {true, _}, State) when Mover =:= node() ->
    State1 = loom:emit_after_point(#{type => op, store => [generated, data]}, State),
    loom:maybe_reply(Motion, [Mover, 'did pass chain'], State1);
motion_decided(#{name := Name} = Motion, Mover, Decision, State) ->
    io:format("~p decided ~p for ~p from ~p~n", [node(), Decision, Name, Mover]),
    loom:maybe_reply(Motion, Decision, State).

handle_idle(State) ->
    io:format("idling~n"),
    loom:sleep(State).
