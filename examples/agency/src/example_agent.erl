-module(example_agent).

-behavior(loom).
-export([vsn/1,
         home/1,
         opts/1,
         write_through/3,
         handle_idle/1,
         handle_info/2,
         handle_message/4,
         command_called/4,
         vote_on_motion/3,
         motion_decided/4,
         task_completed/4,
         task_continued/5,
         needs_upgrade/2]).

-export([do_task/3]).

vsn(_) ->
    #{?MODULE => {2, 0, 0}}.

home({?MODULE, Name}) ->
    filename:join([var, url:esc(node()), Name]).

opts({?MODULE, _}) ->
    #{
      idle_timeout => time:timeout({1, minutes}),
      wipe_timeout => time:timeout({5, seconds}),
      sync_interval => 3000 + rand:uniform(3000),
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
    io:format("~p idling~n", [node()]),
    loom:sleep(State).

handle_info(Info, State) ->
    io:format("~p info: ~256p~n", [node(), Info]),
    State.

handle_message(#{do := save}, _Node, true, State) ->
    loom:save(State);
handle_message(#{do := get_state}, _Node, true, State) ->
    State#{response => State};
handle_message(#{do := emit}, _Node, true, State) ->
    loom:wait(loom:stitch_yarn(#{do => reply}, State));
handle_message(#{do := task}, Node, true, State) ->
    Task = {fun ?MODULE:do_task/3, nil},
    loom:wait(loom:stitch_task(task_reply, Node, Task, State));
handle_message(#{do := reply}, _Node, true, State) ->
    State#{response => got_it};
handle_message(#{do := switch}, _Node, true, State) ->
    State1 = loom:alter_reply(fun (switched) -> {ok, switched} end, State),
    loom:switch_message(#{did => switch}, State1);
handle_message(#{did := switch}, _Node, true, State) ->
    State#{response => switched};
handle_message(#{type := start}, Node, true, State) ->
    io:format("~p started~n", [Node]),
    State;
handle_message(#{type := stop}, Node, true, State) ->
    io:format("~p stopped~n", [Node]),
    State;
handle_message(#{write := _}, _, true, State = #{wrote := Wrote}) ->
    State#{response => Wrote};
handle_message(#{make := continue}, _, true, State) ->
    State1 = loom:obtain_continuation(the_frame, State),
    State1#{response => #{
              type => continue,
              token => util:get(State1, continuation)
             }};
handle_message({Param, the_frame}, Node, _, State) ->
    io:format("~p continued with ~p~n", [Node, Param]),
    loom:switch_message(#{switch => 1}, State);
handle_message(#{switch := 1}, _Node, _, State) ->
    loom:switch_message(#{switch => 2}, State);
handle_message(#{switch := 2}, _Node, _, State) ->
    State#{response => 2};
handle_message(Message, Node, true, State) ->
    io:format("~p new message: ~256p~n", [Node, Message]),
    State;
handle_message(_, _, false, State) ->
    State.

command_called(Command, Node, DidChange, State) ->
    io:format("~p command ~256p from ~256p [~p] ~n", [node(), Command, Node, DidChange]),
    State.

vote_on_motion(Motion, Mover, State) ->
    io:format("~p vote on ~256p from ~256p ~n", [node(), Motion, Mover]),
    {{yea, ok}, State}.

motion_decided(#{kind := chain, path := xyz}, Mover, {true, _}, State) when Mover =:= node() ->
    GenVal = base64url:encode(crypto:strong_rand_bytes(8)),
    Modify = #{type => command, verb => modify, kind => chain, path => xyz, value => GenVal},
    State1 = loom:suture_yarn(Modify, State),
    State1#{response => [Mover, 'did pass chain']};
motion_decided(Motion, Mover, Decision, State) ->
    Yarn = util:get(Motion, yarn),
    io:format("~p decided ~256p for ~256p from ~256p~n", [node(), Decision, Yarn, Mover]),
    State.

task_completed(#{name := Name}, Node, Result, State) ->
    io:format("~p completed task ~256p ~256p~n", [Node, Name, Result]),
    State.

task_continued(Name, Reason, {N, T}, _Arg, _State) ->
    Secs = time:timer_elapsed(T) / 1000,
    io:format("[~B @ ~.3fs] ~256p: ~256p~n", [N, Secs, Name, Reason]).

needs_upgrade(Vsn, State) ->
    io:format("~p needs upgrade to vsn ~256p~n", [node(), Vsn]),
    State.

do_task({0, _}, Arg, _State) ->
    {retry, {1, seconds}, Arg};
do_task({1, T}, Arg, _State) ->
    {done, {T, Arg}}.
