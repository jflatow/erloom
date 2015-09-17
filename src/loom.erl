-module(loom).

-export_type([spec/0,
              opts/0,
              message/0,
              reply/0,
              state/0]).

-export_type([decision/0,
              vote/0,
              motion/0,
              ballot/0]).

-type path() :: iodata().
-type spec() :: {atom(), term()}.
-type opts() :: #{
            idle_elapsed => non_neg_integer(),
            idle_timeout => non_neg_integer() | infinity,
            sync_initial => non_neg_integer(),
            sync_interval => pos_integer(),
            unanswered_max => pos_integer()
           }.
-type message() :: #{
               deps => erloom:edge(),
               type => start | stop | motion | ballot | move | task | lookup | modify | remove | term(),
               kind => term(),
               yarn => term()
              }.
-type reply() :: fun((term()) -> term()).
-type state() :: #{
             listener => pid(),
             worker => pid(),
             active => boolean(),                             %% owned by worker
             status => awake | waking | waiting | recovering, %% owned by listener
             spec => spec(),
             home => path(),
             opts => opts(),
             logs => erloom:logs(),
             ours => erloom:which(),
             prior => erloom:edge(),
             front => erloom:edge(),
             edges => erloom:edges(),
             point => erloom:edge(),
             locus => erloom:locus(),
             peers => #{node() => boolean()},
             wrote => {non_neg_integer(), non_neg_integer()},
             cache => #{node() => {pid() | undefined, non_neg_integer()}},
             elect => #{},
             emits => [{term(), message()}],
             tasks => #{term() => {pid() | undefined, {module(), atom(), list()}}},
             reply => #{},
             message => message() | undefined,
             response => term()
            }.

-type vote() :: {yea, term()} | {nay, term()}.
-type decision() :: {boolean(), term()}.
-type motion() :: #{  %% + message
              type => motion,
              kind => conf | chain | term(),
              conf => {add | remove | set, [node()]},
              vote => vote(),
              fiat => decision()
}.
-type ballot() :: #{  %% + message
              type => ballot,
              vote => vote(),
              fiat => decision()
}.

-callback proc(spec()) -> pid().
-callback home(spec()) -> path().
-callback opts(spec()) -> opts().
-callback keep(state()) -> state().
-callback waken(state()) -> state().
-callback verify_message(message(), state()) ->
    {ok, message(), state()} |
    {missing, erloom:edge(), state()} |
    {term(), term(), state()}.
-callback write_through(message(), pos_integer(), state()) ->
    {non_neg_integer(), non_neg_integer()}.
-callback handle_message(message(), node(), boolean(), state()) -> state().
-callback vote_on_motion(motion(), node(), state()) -> {vote(), state()}.
-callback motion_decided(motion(), node(), decision(), state()) -> state().
-callback handle_info(term(), state()) -> state().
-callback handle_idle(state()) -> state().
-callback check_node(node(), pos_integer(), state()) -> state().

-optional_callbacks([proc/1,
                     opts/1,
                     keep/1,
                     waken/1,
                     verify_message/2,
                     write_through/3,
                     handle_message/4,
                     vote_on_motion/3,
                     motion_decided/4,
                     handle_info/2,
                     handle_idle/1,
                     check_node/3]).

-export([send/2,
         send/3,
         call/2,
         call/3,
         move/2,
         move/3,
         state/1,
         multicall/2,
         multicall/3,
         multirecv/1]).

-export([proc/1,
         home/1,
         opts/1,
         path/2,
         keep/1,
         save/1,
         load/1,
         sleep/1,
         waken/1,
         start/2,
         stop/2,
         after_locus/1,
         defer_reply/2,
         maybe_reply/1,
         maybe_reply/2,
         maybe_reply/3,
         unmet_deps/2,
         verify_message/2,
         write_through/3,
         handle_message/4,
         vote_on_motion/3,
         motion_decided/4,
         handle_info/2,
         handle_idle/1,
         check_node/3]).

-export([callback/3,
         callback/4]).

-export([change_peers/2]).

-export([create_yarn/2,
         stitch_yarn/3,
         suture_yarn/3,
         cancel_yarn/2,
         create_task/3,
         make_motion/2,
         maybe_chain/2]).

%% 'send' is the main entry point for communicating with the loom
%% returns the (local) pid for the loom which can be cached
%% cache invalidation however, depends on the loom

send(Spec, Message) ->
    send(Spec, Message, fun (_) -> ok end).

send(Spec, Message, Reply) when not is_pid(Spec) ->
    send(proc(Spec), Message, Reply);
send(Pid, Message, Reply) when is_map(Message), is_function(Reply) ->
    Pid ! {new_message, Message, Reply},
    Pid;
send(Pid, get_state, Reply) when is_function(Reply) ->
    Pid ! {get_state, Reply}.

%% 'call' waits for a reply

call(Spec, Message) ->
    call(Spec, Message, infinity).

call(Spec, Message, Timeout) ->
    Self = self(),
    Ref = make_ref(),
    send(Spec, Message, fun (Result) -> Self ! {Ref, Result} end),
    receive
        {Ref, Result} ->
            Result
    after
        Timeout ->
            {error, timeout}
    end.

%% 'move' signals the node to make a motion

move(Spec, Motion) ->
    move(Spec, Motion, infinity).

move(Spec, Motion, Timeout) ->
    call(Spec, Motion#{type => move}, Timeout).

%% 'state' gets a recent snapshot of the loom state

state(Spec) ->
    call(Spec, get_state).

%% 'multicall' and 'multirecv' conveniently wrap calls to multiple nodes

multicall(Nodes, Args) ->
    multicall(Nodes, Args, 60000).

multicall(Nodes, Args, Timeout) ->
    {Replies, BadNodes} = rpc:multicall(Nodes, loom, multirecv, [Args], Timeout),
    maps:merge(maps:from_list(Replies),
               maps:from_list([{Node, {error, noreply}} || Node <- BadNodes])).

multirecv(Args) ->
    {node(), apply(loom, call, Args)}.

%% 'proc' defines the mechanism for 'find or spawn pid for spec'
%% the default is to use spec as the registry key, which requires erloom app is running
%% can be used as an entry point if we *just* want the pid

proc(Spec) ->
    callback(Spec, {proc, 1}, [Spec], fun () -> erloom_registry:proc(Spec, Spec) end).

%% 'home' is the only strictly required callback

home(Spec) ->
    callback(Spec, {home, 1}, [Spec]).

opts(Spec) ->
    Defaults = #{
      idle_elapsed => 0,
      idle_timeout => infinity,
      sync_initial => time:timer(),
      sync_interval => 60000 + random:uniform(10000), %% stagger for efficiency
      sync_log_limit => 1000,
      sync_push_prob => 0.10,
      unanswered_max => 5
     },
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

path(Tail, #{home := Home}) when is_list(Tail) ->
    filename:join([Home|Tail]);
path(Name, State) ->
    path([Name], State).

keep(State = #{spec := Spec}) ->
    %% keep the builtins that are permanent or cached, plus whatever the callback wants
    %% active is cached: whether we are started / stopped
    %% edges is cached: its where we think other nodes are
    %% point is permanent: exactly where our state is
    %% locus is permanent: the span of the current message
    %% peers is a special case of permanent keys + transient values, we just keep both:
    %%  the keys are the nodes that count as part of our 'cluster', e.g. for writing
    %%  the values tell if the node was counted during the last write through
    %% emits is permanent: messages yet to emit from the current state
    %% tasks is permanent (mostly): tasks which are currently running
    Builtins = maps:with([active, edges, point, locus, peers, emits, elect, tasks], State),
    maps:merge(Builtins, callback(Spec, {keep, 1}, [State], #{})).

save(State) ->
    ok = path:write(path(state, State), term_to_binary(keep(State))),
    State.

kept(State) ->
    binary_to_term(path:read(path(state, State), term_to_binary(#{}))).

load(State = #{home := _}) ->
    Defaults = #{
      active => false,
      edges => #{},
      point => #{},
      peers => #{},
      emits => [],
      tasks => #{},
      cache => #{},
      reply => #{}
     },
    Kept = maps:merge(Defaults, kept(State)),
    State1 = erloom_logs:load(State),
    maps:merge(Kept, State1);
load(State = #{spec := Spec}) ->
    load(State#{home => home(Spec), opts => opts(Spec)}).

sleep(State) ->
    State1 = save(State),
    erloom_logs:close(State1),
    exit(sleep).

waken(State = #{status := awake}) ->
    State;
waken(State = #{spec := Spec}) ->
    %% called on listener once we've caught up to tip, before we accept messages
    %% we were previously not awake, give a chance to liven the state
    State1 = erloom_surety:restart_tasks(State#{status => awake}),
    callback(Spec, {waken, 1}, [State1], State1).

start(_, State = #{active := true}) ->
    State;
start(Node, State) when Node =:= node() ->
    %% node has been asked to participate in the group
    State1 = erloom_surety:launch_tasks(State),
    State1#{active => true};
start(_, State) ->
    State.

stop(_, State = #{active := false}) ->
    State;
stop(Node, State) when Node =:= node() ->
    %% node has been asked to leave the group
    State1 = erloom_surety:pause_tasks(State),
    State1#{active => false};
stop(_, State) ->
    State.

defer_reply(Name, State = #{reply := Replies}) ->
    %% defer the default reply, if any, to name
    %%  and make sure calling default actually calls name
    %% deferred replies are transient, they aren't saved in the state
    case util:get(Replies, default) of
        undefined ->
            State;
        Reply ->
            State#{reply => Replies#{default => Name, Name => Reply}}
    end.

maybe_reply(State) ->
    maybe_reply(util:get(State, response), State).

maybe_reply(Response, State) ->
    maybe_reply(default, Response, State).

maybe_reply(#{yarn := Yarn}, Response, State) ->
    maybe_reply(Yarn, Response, State);
maybe_reply(Reply, Response, State) when is_function(Reply) ->
    Reply(Response),
    State;
maybe_reply(Name, Response, State = #{reply := Replies}) ->
    %% try and reply to the deferred reply by name
    %% delete reply, so it can only happen once
    case util:get(Replies, Name) of
        undefined ->
            State;
        Reply ->
            maybe_reply(Reply, Response, util:remove(State, [reply, Name]))
    end.

after_locus(#{locus := Locus}) ->
    erloom:locus_after(Locus).

unmet_deps(#{deps := Deps}, #{point := Point}) ->
    case erloom:edge_delta(Deps, Point) of
        Delta when map_size(Delta) > 0 ->
            erloom:delta_upper(Delta);
        _ ->
            nil
    end;
unmet_deps(_Message, _State) ->
    nil.

ready_to_accept(#{type := start}, _State) ->
    {true, start};
ready_to_accept(_Message, #{status := awake, active := true}) ->
    {true, running};
ready_to_accept(_Message, #{status := awake}) ->
    {false, stopped};
ready_to_accept(_Message, #{status := Status}) ->
    {false, Status}.

verify_message(Message, State = #{spec := Spec}) ->
    %% should we even accept the message?
    %% at a minimum, we must be ready to accept, and any dependencies must be met
    case ready_to_accept(Message, State) of
        {true, _} ->
            case unmet_deps(Message, State) of
                nil ->
                    callback(Spec, {verify_message, 2}, [Message, State], {ok, Message, State});
                Deps ->
                    {missing, Deps, State}
            end;
        {false, stopped} ->
            {error, stopped, State};
        {false, Status} ->
            {retry, Status, State}
    end.

write_through(Message, N, State = #{spec := Spec}) ->
    %% how many copies of a message are required for a successful write? in what timeframe?
    callback(Spec, {write_through, 3}, [Message, N, State], {1, infinity}).

handle_builtin(#{type := start, seed := Nodes}, Node, State) ->
    %% the seed message sets the initial electorate, and thus the peer group
    %% every node in the group should see / refer to the same seed message
    erloom_electorate:create(Nodes, start(Node, State));
handle_builtin(Message = #{type := start}, Node, State) ->
    erloom_electorate:affirm_config(Message, Node, start(Node, State));
handle_builtin(Message = #{type := stop}, Node, State) ->
    erloom_electorate:affirm_config(Message, Node, stop(Node, State));
handle_builtin(Message = #{type := motion}, Node, State) ->
    erloom_electorate:handle_motion(Message, Node, State);
handle_builtin(Message = #{type := ballot}, Node, State) ->
    erloom_electorate:handle_ballot(Message, Node, State);
handle_builtin(Message = #{type := move}, Node, State) when Node =:= node() ->
    %% its convenient to be able to push a node to make a motion
    erloom_electorate:motion(Message, State);
handle_builtin(Message = #{type := task}, Node, State) when Node =:= node() ->
    %% task messages are for the surety to either retry or complete a task
    %% tasks run per node, transferring control is outside the scope
    erloom_surety:handle_task(Message, State);
handle_builtin(#{type := lookup, path := Path, kind := chain}, _Node, State) ->
    State#{response => element(1, erloom_chain:lookup(State, Path))};
handle_builtin(#{type := lookup, path := Path}, _Node, State) ->
    State#{response => util:lookup(State, Path)};
handle_builtin(#{type := modify, path := Path, value := Value, kind := chain}, _Node, State) ->
    erloom_chain:modify(State, Path, {Value, after_locus(State)});
handle_builtin(#{type := modify, path := Path, value := Value}, _Node, State) ->
    util:modify(State, Path, Value);
handle_builtin(#{type := remove, path := Path}, _Node, State) ->
    util:remove(State, Path);
handle_builtin(_Message, _Node, State) ->
    State.

handle_message(Message, Node, IsNew, State = #{spec := Spec}) ->
    %% called to transform state for every message on every node
    %% happens 'at least once', should be externally idempotent
    %% for 'at most once', check if is new
    State1 = cancel_yarn(Message, State),
    State2 = handle_builtin(Message, Node, State1),
    Respond = fun () -> maybe_reply(State2) end,
    callback(Spec, {handle_message, 4}, [Message, Node, IsNew, State2], Respond).

vote_on_motion(Motion, Mover, State = #{spec := Spec}) ->
    callback(Spec, {vote_on_motion, 3}, [Motion, Mover, State], {{yea, ok}, State}).

motion_decided(Motion, Mover, Decision, State = #{spec := Spec}) ->
    callback(Spec, {motion_decided, 4}, [Motion, Mover, Decision, State], State).

handle_info(Info, State = #{spec := Spec}) ->
    callback(Spec, {handle_info, 2}, [Info, State], State).

handle_idle(State = #{spec := Spec}) ->
    callback(Spec, {handle_idle, 1}, [State], fun () -> sleep(State) end).

check_node(Node, Unanswered, State = #{spec := Spec}) ->
    callback(Spec, {check_node, 3}, [Node, Unanswered, State], State).

callback({Mod, _}, {Fun, _}, Args) when is_atom(Mod) ->
    erlang:apply(Mod, Fun, Args).

callback({Mod, _}, {Fun, Arity}, Args, Default) when is_atom(Mod) ->
    case erlang:function_exported(Mod, Fun, Arity) of
        true ->
            erlang:apply(Mod, Fun, Args);
        false when is_function(Default) ->
            Default();
        false ->
            Default
    end.

change_peers({set, Nodes}, State) ->
    change_peers({add, Nodes}, State#{peers => #{}});

change_peers({add, [Node|Rest]}, State) when Node =:= node() ->
    change_peers({add, Rest}, State);
change_peers({add, [Node|Rest]}, State) ->
    change_peers({add, Rest}, util:ifndef(State, [peers, Node], false));

change_peers({remove, [Node|Rest]}, State) when Node =:= node() ->
    change_peers({remove, Rest}, State);
change_peers({remove, [Node|Rest]}, State) ->
    change_peers({remove, Rest}, util:remove(State, [peers, Node]));

change_peers({_, []}, State) ->
    State.

%% Yarns serve 3 purposes:
%%  1. Deferring replies to a message
%%  2. Generating new information, and communicating it to the group
%%  3. Preventing the duplicate generation of new information

create_yarn(Message = #{yarn := Yarn}, State) ->
    stitch_yarn(Yarn, Message, defer_reply(Yarn, State));
create_yarn(Message, State) ->
    %% by default the name of the yarn is based on the current locus
    %% NB: don't rely on the default name if creating multiple yarns
    create_yarn(Message#{yarn => after_locus(State)}, State).

stitch_yarn(#{yarn := Yarn}, Message, State) ->
    stitch_yarn(Yarn, Message, State);
stitch_yarn(Yarn, Message, State = #{emits := Emits}) ->
    %% only one message pending per yarn at a time
    State#{emits => util:set(Emits, {node(), Yarn}, Message#{yarn => Yarn})}.

suture_yarn(Yarned, Message, State = #{point := Point}) ->
    %% emit a message that depends on the entire current context
    %% this is the strongest set of deps we can possibly have here
    %% NB: reserve the right to treat builtin types (i.e. motion) differently
    %%     e.g. only needs to depend on ratifying votes instead of full point
    Deps = erloom:edge_hull(Point, after_locus(State)),
    stitch_yarn(Yarned, Message#{deps => Deps}, State).

cancel_yarn(#{yarn := Yarn}, State = #{emits := Emits}) ->
    State#{emits => util:delete(Emits, {node(), Yarn})};
cancel_yarn(_, State) ->
    State.

create_task(Name, Task, State) ->
    %% tasks have similar "defer until tip" semantics as emit messages
    %% however processes must be restored on wake, making them more complicated
    erloom_surety:task(Name, Task, State).

make_motion(Motion, State) ->
    erloom_electorate:motion(Motion, State).

maybe_chain(Change, State) ->
    erloom_electorate:tether(Change, State).
