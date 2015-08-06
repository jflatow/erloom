-module(agent).

-behavior(loom).
-export([home/1,
         opts/1,
         pure_effects/3,
         side_effects/4,
         write_through/2]).

home({agent, Name}) ->
    filename:join([var, url:esc(node()), Name]).

opts({agent, _}) ->
    #{sync_interval => 3000 + random:uniform(3000)}.

pure_effects(#{set_peers := Peers}, _Node, State) ->
    loom:set_peers(Peers, State);
pure_effects(Message, Node, State) ->
    io:format("pure effects: ~p ~p~n", [Message, Node]),
    State.

side_effects(_Message, Reply, _State, State) ->
    Reply(State),
    State.

write_through(#{command := get_state}, _State) ->
    {0, infinity};
write_through(_Message, _State) ->
    {1, 30000}.
