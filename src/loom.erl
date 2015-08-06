-module(loom).

-export_type([spec/0,
              opts/0,
              state/0]).

-type path() :: iodata().
-type spec() :: {atom(), term()}.
-type opts() :: #{
            idle_elapsed => non_neg_integer(),
            idle_timeout => non_neg_integer() | infinity,
            sync_initial => non_neg_integer(),
            sync_interval => pos_integer(),
            unanswered_max => pos_integer()
           }.
-type logs() :: #{node() => erloom:log()}.
-type message() :: #{deps => erloom:edge()}.
-type reply() :: fun((term()) -> term()).
-type state() :: #{
             listener => pid(),
             worker => pid(),
             spec => spec(),
             home => path(),
             opts => opts(),
             logs => logs(),
             prior => erloom:edge(),
             front => erloom:edge(),
             edges => erloom:edges(),
             point => erloom:edge(),
             cache => #{node() => {pid() | undefined, non_neg_integer()}},
             peers => #{node() => {non_neg_integer(), non_neg_integer()}},
             wrote => boolean()
            }.

-callback proc(spec()) -> pid().
-callback home(spec()) -> path().
-callback opts(spec()) -> opts().
-callback keep(state()) -> state().
-callback verify_message(message(), state()) ->
    {ok, message(), state()} |
    {missing, erloom:edge(), state()} |
    {error, term(), state()}.
-callback write_through(message(), state()) ->
    {non_neg_integer(), non_neg_integer()} |
    fun((pos_integer()) -> {non_neg_integer(), non_neg_integer()}).
-callback pure_effects(message(), node(), state()) -> state().
-callback side_effects(message(), reply(), state(), state()) -> state().
-callback handle_info(term(), state()) -> state().
-callback handle_idle(state()) -> state().
-callback emit_message(state()) -> message() | nil.
-callback check_node(node(), pos_integer(), state()) -> state().

-optional_callbacks([opts/1,
                     proc/1,
                     keep/1,
                     verify_message/2,
                     write_through/2,
                     pure_effects/3,
                     side_effects/4,
                     handle_info/2,
                     handle_idle/1,
                     emit_message/1,
                     check_node/3]).

-export([proc/1,
         send/2,
         send/3,
         call/2,
         call/3,
         home/1,
         opts/1,
         path/2,
         keep/1,
         save/1,
         load/1,
         sleep/1,
         delegate/1,
         verify_message/2,
         write_through/2,
         pure_effects/3,
         side_effects/4,
         handle_info/2,
         handle_idle/1,
         emit_message/1,
         check_node/3]).

-export([callback/3,
         callback/4]).

-export([add_peers/2,
         remove_peers/2,
         set_peers/2]).

-export([point_to_front/2,
         unmet_deps/2,
         obtain_log/2,
         extend_log/4,
         replay_log/4,
         write_log/2]).

%% 'proc' defines the mechanism for 'find or spawn pid for spec'
%% the default is to use spec as the registry key, which requires erloom app is running
%% can be used as an entry point if we *just* want the pid

proc(Spec) ->
    callback(Spec, {proc, 1}, [Spec], fun () -> erloom_registry:proc(Spec, Spec) end).

%% 'send' is the main entry point for communicating with the loom
%% returns the (local) pid for the loom which can be cached
%% cache invalidation however, depends on the loom

send(Spec, Message) ->
    send(Spec, Message, fun (_) -> ok end).

send(Spec, Message, Reply) when not is_pid(Spec) ->
    send(proc(Spec), Message, Reply);
send(Pid, Message, Reply) when is_map(Message), is_function(Reply) ->
    Pid ! {new_message, Message, Reply},
    Pid.

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
      unanswered_max => 5
     },
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

path(Tail, #{home := Home}) when is_list(Tail) ->
    filename:join([Home|Tail]);
path(Name, State) ->
    path([Name], State).

keep(State = #{spec := Spec}) ->
    %% keep the builtins that are permanent or cached, plus whatever the callback wants
    %% edges is cached: its where we think others nodes are
    %% point is permanent: exactly its where our state is
    %% peers is a special case of permanent keys + transient values, we just keep both:
    %%  the keys are the nodes that count as part of our 'cluster', e.g. for writing
    %%  the values tell if the node was counted during the last write through
    Builtins = maps:with([edges, point, peers], State),
    maps:merge(Builtins, callback(Spec, {keep, 1}, [State], #{})).

save(State) ->
    ok = path:write(path(state, State), term_to_binary(keep(State))),
    State.

kept(State) ->
    binary_to_term(path:read(path(state, State), term_to_binary(#{}))).

logs(State) ->
    lists:foldl(fun (Name, Logs) ->
                        {ok, Log} = log:open(path([logs, Name], State)),
                        Node = util:atom(url:unescape(Name)),
                        Logs#{Node => Log}
                end, #{}, path:list(path(logs, State))).

load(State = #{home := _}) ->
    Kept = kept(State),
    Logs = logs(State),
    State1 = State#{
               logs => Logs,
               front => util:map(Logs, fun log:locus/1),
               edges => util:get(Kept, edges, #{}),
               point => util:get(Kept, point, #{}),
               peers => util:get(Kept, peers, #{}),
               cache => #{}
              },
    maps:merge(Kept, State1);
load(State = #{spec := Spec}) ->
    load(State#{home => home(Spec), opts => opts(Spec)}).

sleep(State) ->
    save(State),
    exit(sleep).

delegate(#{listener := Listener}) ->
    {node(), Listener}.

verify_message(Message, State = #{spec := Spec}) ->
    %% should we even accept the message?
    %% at a minimum, any dependencies must be met
    case unmet_deps(Message, State) of
        nil ->
            callback(Spec, {verify_message, 2}, [Message, State], {ok, Message, State});
        Deps ->
            {missing, Deps, State}
    end.

write_through(Message, State = #{spec := Spec}) ->
    %% how many copies of a message are required for a successful write? in what timeframe?
    callback(Spec, {write_through, 2}, [Message, State], {1, infinity}).

pure_effects(Message, Node, State = #{spec := Spec}) ->
    %% these effects will be applied to the state on every node
    %% they should be externally idempotent, as they happen 'at least once'
    callback(Spec, {pure_effects, 3}, [Message, Node, State], State).

side_effects(Message, Reply, State0 = #{spec := Spec}, State1) ->
    %% these effects will only be applied when a new message is received
    %% it is possible they won't happen at all, they are 'at most once'
    callback(Spec, {side_effects, 4}, [Message, Reply, State0, State1], State1).

handle_info({'EXIT', Listener, Reason}, #{listener := Listener, worker := Worker}) ->
    exit(Worker, Reason);
handle_info({'EXIT', Worker, Reason}, #{listener := Listener, worker := Worker}) ->
    exit(Listener, Reason);
handle_info(Info, State = #{spec := Spec}) ->
    callback(Spec, {handle_info, 2}, [Info, State], State).

handle_idle(State = #{spec := Spec}) ->
    callback(Spec, {handle_idle, 1}, [State], State).

emit_message(State = #{spec := Spec}) ->
    callback(Spec, {emit_message, 1}, [State], nil).

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

add_peers([Node|Rest], State) when Node =:= node() ->
    add_peers(Rest, State);
add_peers([Node|Rest], State) ->
    add_peers(Rest, util:ifndef(State, [peers, Node], false));
add_peers([], State) ->
    State.

remove_peers([Node|Rest], State) when Node =:= node() ->
    remove_peers(Rest, State);
remove_peers([Node|Rest], State) ->
    remove_peers(Rest, util:remove(State, [peers, Node]));
remove_peers([], State) ->
    State.

set_peers(Nodes, State) ->
    add_peers(Nodes, State#{peers => #{}}).

point_to_front(Node, State) ->
    util:modify(State, [point, Node], util:lookup(State, [front, Node])).

unmet_deps(#{deps := Deps}, #{point := Point}) ->
    case erloom:edge_delta(Deps, Point) of
        Delta when map_size(Delta) > 0 ->
            erloom:delta_upper(Delta);
        _ ->
            nil
    end;
unmet_deps(_Message, _State) ->
    nil.

obtain_log(Node, State = #{logs := Logs}) ->
    case util:get(Logs, Node) of
        undefined ->
            {ok, Log} = log:open(path([logs, url:esc(Node)], State)),
            {Log, State#{logs => Logs#{Node => Log}}};
        Log ->
            {Log, State}
    end.

extend_log(Log, [{{_, After} = Range, Data}|Rest], Node, State = #{front := Front}) ->
    {ok, Range} = log:write(Log, Data, call),
    extend_log(Log, Rest, Node, State#{front => Front#{Node => After}});
extend_log(_, [], _, State) ->
    State.

replay_log(Fun, Range, Node, State) ->
    {Log, State1} = obtain_log(Node, State),
    {{_, After}, State2 = #{point := Point}} =
        log:bendl(Log,
                  fun ({{Before, _}, Data}, S = #{point := P}) ->
                          %% NB: we mark Before in case Fun decides to exit early
                          Fun(binary_to_term(Data), Node, S#{point => P#{Node => Before}})
                  end, State1, Range),
    State2#{point => Point#{Node => After}}.

write_log(Message, State) when is_map(Message) ->
    write_log(term_to_binary(Message), State);
write_log(Data, State = #{front := Front}) ->
    {Log, State1} = obtain_log(node(), State),
    {ok, {_, After} = Range} = log:write(Log, Data, call),
    {[{Range, Data}], State1#{front => Front#{node() => After}}}.
