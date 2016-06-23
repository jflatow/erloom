-module(loom).
-author("Jared Flatow").

-export_type([spec/0,
              home/0,
              opts/0,
              message/0,
              reply/0,
              state/0]).

-export_type([decision/0,
              vote/0,
              motion/0,
              ballot/0]).

-type spec() :: {atom(), term()} | state().
-type home() :: iodata().
-type opts() :: #{
            idle_elapsed => non_neg_integer(),                %% state, not really opt
            idle_timeout => non_neg_integer() | infinity,
            wipe_timeout => non_neg_integer(),
            sync_initial => non_neg_integer(),                %% state, not really opt
            sync_interval => pos_integer(),
            sync_log_limit => pos_integer(),
            sync_push_prob => float(),
            task_limit => pos_integer() | infinity,
            task_wait_for => pos_integer(),
            unanswered_max => pos_integer()
           }.

-type vsn() :: #{atom() => tuple() | undefined}.
-type message() :: #{
               vsn => vsn(),
               deps => erloom:edge(),
               refs => erloom:loci(),
               type => (upgrade | save | sleep | start | stop |
                        command | motion | ballot | ratify |
                        move | tether | fence | task | continue | atom()),
               kind => atom(),
               yarn => term(),
               mute => boolean()
              }.
-type reply() :: fun((term()) -> term()) | undefined.
-type state() :: #{
             listener => pid(),
             worker => pid(),
             active => boolean(),                             %% owned by worker
             status => awake | waking | waiting | recovering, %% owned by listener
             vsn => vsn(),                                    %% vsn of the current code
             vsns => #{node() => vsn()},                      %% vsn info at point
             spec => spec(),
             home => home(),
             opts => opts(),
             logs => erloom:logs(),
             ours => erloom:which(),
             prior => erloom:edge(),
             front => erloom:edge(),                          %% edge after logs written
             edges => erloom:edges(),                         %% edges of everyone else
             point => erloom:edge(),                          %% edge after last handled
             dirty => erloom:edge(),                          %% edge after last save
             locus => erloom:locus(),
             names => [],
             peers => #{node() => boolean()},
             wrote => {non_neg_integer(), non_neg_integer()},
             cache => #{node() => {pid() | undefined, non_neg_integer()}},
             elect => #{},
             emits => [{term(), message()} | {term(), message(), reply()}],
             tasks => #{term() => {pid() | undefined, {module(), atom(), list()}}},
             reply => #{},
             message => message() | undefined,
             response => term(),
             former => term(),
             tokens => #{}
            }.

-type command() :: #{ %% + message()
               kind => chain | batch | atom(),
               verb => lookup | modify | accrue | create | remove | swap | atom(),
               path => atom() | list(),
               value => term()
              }.

-type vote() :: {yea, term()} | {nay, term()}.
-type decision() :: {boolean(), term()}.
-type motion() :: #{  %% + message()
              type => motion,
              kind => conf | chain | batch | atom(),
              conf => root | undefined,
              fiat => decision()
             }.
-type ballot() :: #{  %% + message()
              type => ballot,
              vote => vote(),
              fiat => decision()
             }.

-type context() :: #{}. %% NB: an in/out parameter for 2-way communication

-callback vsn(spec()) -> vsn().
-callback find(spec(), context()) ->
    {ok, {spec(), [node()]}, context()} |
    {error, term(), context()}.
-callback proc(spec()) -> pid().
-callback home(spec()) -> home().
-callback opts(spec()) -> opts().
-callback keep(state()) -> state().
-callback init(state()) -> state().
-callback waken(state()) -> state().
-callback verify_message(message(), state()) ->
    {ok, message(), state()} |
    {missing, erloom:edge(), state()} |
    {term(), term(), state()}.
-callback write_through(message(), pos_integer(), state()) ->
    {non_neg_integer(), non_neg_integer()}.
-callback handle_idle(state()) -> state().
-callback handle_info(term(), state()) -> state().
-callback handle_message(message() | {term(), term()}, node(), boolean(), state()) -> state().
-callback command_called(command(), node(), boolean(), state()) -> state().
-callback vote_on_motion(motion(), node(), state()) -> {vote(), state()}.
-callback motion_decided(motion(), node(), decision(), state()) -> state().
-callback task_completed(message(), node(), term(), state()) -> state().
-callback task_continued(term(), term(), term(), tuple(), state()) -> any().
-callback check_node(node(), pos_integer(), state()) -> state().
-callback needs_upgrade(vsn(), state()) -> state().

-optional_callbacks([vsn/1,
                     find/2,
                     proc/1,
                     opts/1,
                     keep/1,
                     init/1,
                     waken/1,
                     verify_message/2,
                     write_through/3,
                     handle_idle/1,
                     handle_info/2,
                     handle_message/4,
                     command_called/4,
                     vote_on_motion/3,
                     motion_decided/4,
                     task_completed/4,
                     task_continued/5,
                     check_node/3,
                     needs_upgrade/2]).

-export([seed/1,
         seed/2,
         seed/3,
         send/2,
         send/3,
         call/2,
         call/3,
         state/1,
         ok/1,
         rpc/3,
         rpc/4,
         multirpc/3,
         multirpc/4,
         multircv/3,
         drive/1,
         drive/2,
         steer/1,
         steer/2,
         patch/2,
         patch/3,
         dispatch/2,
         dispatch/3,
         deliver/3,
         deliver/4]).

-export([vsn/1,
         find/2,
         proc/1,
         home/1,
         opts/1,
         path/2,
         keep/1,
         save/1,
         load/1,
         init/1,
         waken/1,
         sleep/1,
         start/2,
         stop/2,
         wipe/1,
         clean/1,
         after_locus/1,
         after_point/1,
         alter_reply/2,
         alter_reply/3,
         defer_reply/2,
         eject_reply/1,
         eject_reply/2,
         maybe_reply/1,
         maybe_reply/2,
         maybe_reply/3,
         wait/1,
         unmet_needs/2,
         unmet_needs/3,
         verify_message/2,
         write_through/3,
         handle_idle/1,
         handle_info/2,
         handle_message/4,
         command_called/4,
         vote_on_motion/3,
         motion_decided/4,
         task_completed/4,
         task_continued/5,
         check_node/3,
         needs_upgrade/2]).

-export([callback/3,
         callback/4]).

-export([change_peers/2]).

%% emissions
-export([emit/2,
         switch_message/2,
         switch_unmuted/2,
         stitch_yarn/2,
         suture_yarn/2,
         stitch_task/4,
         suture_task/4,
         make_motion/2,
         make_tether/2]).

%% continuations
-export([create_continuation/2,
         create_continuation/3,
         obtain_continuation/2,
         obtain_continuation/3]).

%% token management
-export([is_valid_token/2,
         create_token/3,
         forget_token/2,
         purge_tokens/1]).

%% 'seed' (or equivalent) must be called before a loom can be used
%% it is called only the first time the loom is *ever* used,
%% assuming the message is persisted (write_through not overridden)

seed(Specish) ->
    case find(Specish, #{}) of
        {ok, {Spec, Nodes}, _} ->
            seed(Spec, Nodes);
        {error, Reason, _} ->
            {error, {find, Reason}}
    end.

seed(Spec, Nodes) ->
    seed(Spec, Nodes, []).

seed(Spec, Nodes, Opts) ->
    call(Spec, #{type => start, seed => Nodes}, Opts).

%% 'send' is the main entry point for communicating with the loom
%% returns the (local) pid for the loom which can be cached
%% cache invalidation however, depends on the loom

send(Spec, Message) ->
    send(Spec, Message, fun (_) -> ok end).

send(#{listener := Listener}, Message, Reply) ->
    send(Listener, Message, Reply);
send(Spec, Message, Reply) when not is_pid(Spec) ->
    send(proc(Spec), Message, Reply);
send(Pid, Message, Reply) when is_map(Message), is_function(Reply) ->
    Pid ! {new_message, Message, Reply},
    Pid;
send(Pid, get_state, Reply) when is_function(Reply) ->
    Pid ! {get_state, Reply}.

%% 'call' waits for a reply

call(Spec, Message) ->
    call(Spec, Message, []).

call(Spec, Message, Opts) ->
    Timeout = util:get(Opts, timeout, 3000),
    Self = self(),
    Ref = make_ref(),
    send(Spec, Message, fun (Result) -> Self ! {Ref, Result} end),
    receive
        {Ref, Result} ->
            Result
    after
        Timeout ->
            {wait, timeout}
    end.

%% 'state' gets a recent snapshot of the loom state (i.e. for debugging)

state(Specish) ->
    patch(Specish, get_state).

%% 'ok' extracts normal responses and turns abnormal responses into errors

ok({ok, Normal, _}) ->
    Normal;
ok({throw, Error, Ctx}) ->
    throw({Error, Ctx});
ok({error, Error, Ctx}) ->
    error({Error, Ctx}).

%% 'rpc' conveniently wrap calls to looms on other nodes

rpc(Node, Spec, Message) ->
    rpc(Node, Spec, Message, []).

rpc(Node, Spec, Message, Opts) ->
    {Timeout, Opts1} = util:default(Opts, [timeout], 30000),
    rpc:call(Node, loom, call, [Spec, Message, Opts1], Timeout).

%% 'multirpc' and 'multircv' conveniently wrap calls to multiple nodes

multirpc(Nodes, Spec, Message) ->
    multirpc(Nodes, Spec, Message, []).

multirpc(Nodes, Spec, Message, Opts) ->
    {Timeout, Opts1} = util:default(Opts, [timeout], 30000),
    {Replies, BadNodes} = rpc:multicall(Nodes, loom, multircv, [Spec, Message, Opts1], Timeout),
    maps:merge(maps:from_list(Replies), util:mapped(BadNodes, {error, noreply})).

multircv(Spec, Message, Opts) ->
    {node(), apply(loom, call, [Spec, Message, Opts])}.


%% 'drive' and 'steer' allow the callee to direct the caller what to do next
%% so the client need not know how to do anything in particular, like a user-agent
%% basically a trampoline for looms
%% differentiates between error, exception, continuation, and return:
%%   {ok, Return, _}
%%   {ok, Continue := #{which := _, message := _, [return := Override]}, _}
%%   {_, Exception := #{error := _}, _}
%%   {error, Error, _}
%% NB: a loom should not call itself, or it may block indefinitely
%%     same applies if drive is called from a loom and it is redirected to itself

drive(Delivery) ->
    drive(Delivery, undefined).

drive({ok, Result, Ctx}, Return) ->
    case steer(Result, Ctx) of
        {ok, Value, Ctx1} when Return =:= undefined ->
            {ok, Value, Ctx1};
        {ok, Value, Ctx1}  when is_function(Return) ->
            {ok, Return(Value), Ctx1};
        {ok, _, Ctx1} ->
            {ok, Return, Ctx1};
        Abnormal ->
            Abnormal
    end;
drive({error, Throw = #{error := _}, Ctx}, _) ->
    {throw, Throw, Ctx};
drive({error, Error, Ctx}, _) ->
    {error, Error, Ctx}.

steer(Instruction) ->
    loom:ok(steer(Instruction, #{})).

steer(Throw = #{error := _}, Ctx) ->
    {throw, Throw, Ctx};
steer(Patch = #{which := Which, message := Message}, Ctx) ->
    drive(loom:patch(Which, Message, Ctx), util:get(Patch, return));
steer(Other, Ctx) ->
    {ok, Other, Ctx}.

%% 'patch' dispatches a message to a loom, but flattens the response
%% in the case of another patch 3-tuple, throw away the subcontext for convenience
%% the callee can always explicitly deal with it or pack it into the response

patch(Specish, Message) ->
    loom:ok(patch(Specish, Message, #{})).

patch(Specish, Message, Ctx) ->
    case dispatch(Specish, Message, Ctx) of
        {ok, {ok, Response}, Ctx1} ->
            {ok, Response, Ctx1};
        {ok, {ok, Response, _Ctx}, Ctx1} ->
            {ok, Response, Ctx1};
        {ok, {error, Reason}, Ctx1} ->
            {error, Reason, Ctx1};
        {ok, {error, Reason, _Ctx}, Ctx1} ->
            {error, Reason, Ctx1};
        {ok, Response, Ctx1} ->
            {ok, Response, Ctx1};
        {error, Reason, Ctx1} ->
            {error, Reason, Ctx1}
    end.

%% 'dispatch' tries to find and deliver a message to a loom

dispatch(Specish, Message) ->
    loom:ok(dispatch(Specish, Message, #{})).

dispatch(Specish, Message, Ctx) ->
    case find(Specish, Ctx) of
        {ok, {Spec, Nodes}, Ctx1, FollowUp} ->
            Delivery = deliver(Nodes, Spec, Message, Ctx1),
            FollowUp(Specish, Message, Delivery);
        {ok, {Spec, Nodes}, Ctx1} ->
            deliver(Nodes, Spec, Message, Ctx1);
        {error, Reason, Ctx1} ->
            {error, Reason, Ctx1}
    end.

%% 'deliver' tries to get a message through to one of the nodes for a loom
%% it tries the nodes randomly, starting with a preferred node, if given
%% we retry as many times as desired, or the caller can retry

deliver(Nodes, Spec, Message) ->
    loom:ok(deliver(Nodes, Spec, Message, #{})).

deliver(Nodes, Spec, Message, Ctx) ->
    deliver(Nodes, Spec, Message, Ctx, [], Nodes).

deliver(Nodes, Spec, Message, Ctx, Tried, Remaining) ->
    %% NB: we can't be certain messages never reach a loom, so they should be idempotent
    case util:draw(Remaining, util:get(Ctx, pref, node())) of
        {undefined, _} ->
            case util:get(Ctx, retry, {0, infinity}) of
                {infinity, T} ->
                    receive after T -> deliver(Nodes, Spec, Message, Ctx) end;
                {N, T} when is_integer(N), N > 0 ->
                    Ctx1 = util:set(Ctx, retry, {N - 1, T}),
                    receive after T -> deliver(Nodes, Spec, Message, Ctx1) end;
                _ ->
                    {error, delivery,
                     util:set(Ctx, dstat,
                              #{
                                 spec => Spec,
                                 nodes => Nodes,
                                 tried => Tried,
                                 message => Message
                               })}
            end;
        {Node, R1} ->
            Response = rpc(Node, Spec, Message, Ctx),
            T1 = util:set(Tried, Node, Response),
            case Response of
                {badrpc, _} ->
                    %% the message never even got to the loom
                    deliver(Nodes, Spec, Message, Ctx, T1, R1);
                {retry, _} ->
                    %% the loom didn't accept the message
                    deliver(Nodes, Spec, Message, Ctx, T1, R1);
                {wait, timeout} ->
                    %% timeout is considered a failure during delivery
                    deliver(Nodes, Spec, Message, Ctx, T1, R1);
                _ ->
                    %% any other response from the loom is considered received
                    {ok, Response,
                     util:set(Ctx, dstat,
                              #{
                                 spec => Spec,
                                 node => Node,
                                 nodes => Nodes,
                                 tried => T1,
                                 message => Message
                               })}
            end
    end.

%% 'vsn' tells the version of the code
%% looms will refuse to process messages if they don't have a high enough vsn
%% vsns are like edges, lacking any part of required vsn will require an upgrade

vsn(Spec) ->
    maps:merge(callback(Spec, {vsn, 1}, [Spec], #{}), #{}).

%% 'find' takes a spec or something similar and returns the exact spec and nodes
%% when it's unknown which nodes a loom is running on, this is where to look
%% can block for an arbitrarily long time, and may return an error
%% its up to the callback module to define what other forms of spec are accepted

find(#{spec := Spec}, Ctx) ->
    find(Spec, Ctx);
find(Specish, Ctx) ->
    callback(Specish, {find, 2}, [Specish, Ctx], {ok, {Specish, [node()]}, Ctx}).

%% 'proc' defines the mechanism for getting or creating the pid for a spec
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
      wipe_timeout => time:timeout({30, minutes}),
      sync_initial => time:timer(),
      sync_interval => 60000 + random:uniform(10000), %% stagger for efficiency
      sync_log_limit => 1000,
      sync_push_prob => 0.10,
      task_limit => 100,
      task_wait_for => 1000,
      unanswered_max => 2
     },
    maps:merge(Defaults, callback(Spec, {opts, 1}, [Spec], #{})).

path(Tail, #{home := Home}) when is_list(Tail) ->
    filename:join([Home|Tail]);
path(Name, State) ->
    path([Name], State).

keep(State) ->
    %% keep the builtins that are permanent or cached, plus whatever the callback wants
    %% active is cached: whether we are started / stopped
    %% edges is cached: its where we think other nodes are
    %% point is permanent: exactly where our state is
    %% locus is permanent: the span of the current message
    %% names is permanent: reserved for managers
    %% peers is a special case of permanent keys + transient values, we just keep both:
    %%  the keys are the nodes that count as part of our 'cluster', e.g. for writing
    %%  the values tell if the node was counted during the last write through
    %% emits is permanent: messages yet to emit from the current state
    %% tasks is permanent (mostly): tasks which are currently running
    Builtins = maps:with([active,
                          vsns,
                          edges,
                          point,
                          locus,
                          names,
                          peers,
                          emits,
                          elect,
                          tasks,
                          tokens], State),
    maps:merge(Builtins, callback(State, {keep, 1}, [State], #{})).

save(State = #{point := Point, dirty := Point}) ->
    %% to force a save, one could simply set dirty => true
    State;
save(State) ->
    ok = path:write(path(state, State), term_to_binary(keep(State))),
    clean(State).

kept(State) ->
    binary_to_term(path:read(path(state, State), term_to_binary(#{}))).

load(State = #{home := _}) ->
    Defaults = #{
      active => false,
      edges => #{},
      point => #{},
      names => [],
      peers => #{},
      emits => [],
      tasks => #{},
      cache => #{},
      reply => #{},
      tokens => #{}
     },
    Kept = maps:merge(Defaults, kept(State)),
    State1 = erloom_logs:load(State),
    clean(maps:merge(Kept, State1#{vsn => vsn(State1)}));
load(State = #{spec := Spec}) ->
    load(State#{home => home(Spec), opts => opts(Spec)}).

init(State) ->
    %% called on listener after we are loaded, but before we replay any messages
    %% we were previously not loaded, give a chance to do early initialization
    callback(State, {init, 1}, [State], State).

waken(State = #{status := awake}) ->
    State;
waken(State) ->
    %% called on listener once we've caught up to tip, before we accept messages
    %% we were previously not awake, give a chance to liven the state
    State1 = erloom_surety:restart_tasks(State#{status => awake}),
    State2 = maybe_upgrade(State1),
    State3 = purge_tokens(State2),
    callback(State3, {waken, 1}, [State3], State3).

sleep(State) ->
    State1 = save(State),
    erloom_logs:close(State1),
    exit(sleep).

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
    %% allow tasks to finish (which is why task messages are always accepted)
    State#{active => false};
stop(_, State) ->
    State.

wipe(#{home := Home}) ->
    %% when a node is stopped it doesn't idle, it wipes to reclaim disk space
    %% there's a very small possibility that an underinformed node tries to restore us
    %% that's ok, eventually we'll either wipe or get started again
    ok = path:rmrf(Home),
    exit(wiped).

clean(State = #{point := Point}) ->
    %% move dirty up to the point, prevents us from saving when there have been no changes
    State#{dirty => Point}.

maybe_upgrade(State = #{vsn := Vsn}) ->
    %% if the vsn of the code is greater than ours, we should emit a new vsn
    %% noone with a vsn less than ours will be able to process past this point on our log
    %% NB: this happens during waken, so we compare to the vsn we think we are at tip
    case erloom:edge_delta(Vsn, util:lookup(State, [vsns, node()], #{})) of
        Delta when map_size(Delta) > 0 ->
            loom:stitch_yarn(#{type => upgrade, yarn => upgrade, vsn => Vsn}, State);
        _ ->
            State
    end.

alter_reply(Alteration, State) ->
    alter_reply(Alteration, default, State).

alter_reply(Alteration, Name, State = #{reply := Replies}) ->
    case util:get(Replies, Name) of
        undefined ->
            State;
        Reply when is_function(Reply) ->
            State#{reply => Replies#{Name => fun (R) -> Reply(Alteration(R)) end}};
        Reply ->
            alter_reply(Alteration, Reply, State)
    end.

defer_reply(Name, State = #{reply := Replies}) ->
    %% defer the default reply, if any, to name
    %%  and make sure calling default actually calls name
    %% deferred replies are transient, they aren't saved in the state
    case util:get(Replies, default) of
        undefined ->
            State;
        Name ->
            State;
        Reply ->
            State#{reply => Replies#{default => Name, Name => Reply}}
    end.

eject_reply(State) ->
    eject_reply(default, State).

eject_reply(Name, State = #{reply := Replies}) ->
    {Reply, Replies1} = util:pop(Replies, Name),
    {Reply, State#{reply => Replies1}}.

maybe_reply(State) ->
    maybe_reply(util:get(State, response), State).

maybe_reply(Response, State) ->
    maybe_reply(default, Response, State).

maybe_reply(_, undefined, State) ->
    State;
maybe_reply(Reply, Response, State) when is_function(Reply) ->
    Reply(Response),
    State;
maybe_reply(Name, Response, State) ->
    %% try and reply to the deferred reply by name
    %% delete reply, so it can only happen once
    case util:lookup(State, [reply, Name]) of
        undefined ->
            State;
        Reply ->
            maybe_reply(Reply, Response, util:remove(State, [reply, Name]))
    end.

wait(State) ->
    %% setting response to undefined, or removing it, disables the default response
    %% in general this means we are going to respond later, hence the name 'wait'
    util:delete(State, response).

after_locus(#{locus := Locus}) ->
    erloom:locus_after(Locus).

after_point(#{point := Point} = State) ->
    %% after point includes the locus, otherwise just use the point
    erloom:edge_hull(Point, after_locus(State)).

min_deps(Message, #{locus := {{Node, _}, _}}) when Node =:= node() ->
    %% messages on the same node are ordered even without deps
    Message;
min_deps(Message, State) ->
    %% message on another node, extend deps to after locus, minus refs
    Deps = util:get(Message, deps, #{}),
    Refs = util:get(Message, refs, []),
    Deps1 = erloom:edge_hull(Deps, after_locus(State)),
    case erloom:edge_delta(Deps1, erloom:loci_after(Refs)) of
        Delta when map_size(Delta) > 0 ->
            Message#{deps => erloom:delta_upper(Delta)};
        _ ->
            Message
    end.

max_deps(Message, State) ->
    %% this is the strongest set of deps we can possibly have in this context
    Message#{deps => after_point(State)}.

meets_vsn(#{vsn := Vsn}, #{vsn := V}) ->
    case erloom:edge_delta(Vsn, V) of
        Delta when map_size(Delta) > 0 ->
            %% false: only return the parts that need to be upgraded
            {false, erloom:delta_upper(Delta)};
        _ ->
            %% true: return the version that was met (not used anywhere)
            {true, Vsn}
    end;
meets_vsn(#{vsn := Vsn}, _) ->
    {true, Vsn};
meets_vsn(_, _) ->
    {true, #{}}.

unmet_needs(Message, State) ->
    unmet_needs(Message, State, forward).

unmet_needs(Message, State = #{point := Point}, Direction) ->
    case meets_vsn(Message, State) of
        {true, _} ->
            Deps = util:get(Message, deps, #{}),
            Refs = util:get(Message, refs, []),
            All = erloom:edge_hull(Deps, erloom:loci_after(Refs), Direction),
            case erloom:edge_delta(All, Point, Direction) of
                Delta when map_size(Delta) > 0 ->
                    {deps, erloom:delta_bound(Delta, Direction)};
                _ ->
                    nil
            end;
        {false, Vsn} ->
            {vsn, Vsn}
    end.

ready_to_accept(#{type := start}, #{status := S}) when S =:= waiting; S =:= awake ->
    {true, start};
ready_to_accept(#{type := task}, #{status := awake}) ->
    {true, task};
ready_to_accept(_Message, #{status := awake, active := true}) ->
    {true, running};
ready_to_accept(_Message, #{status := awake}) ->
    {false, stopped};
ready_to_accept(_Message, #{status := Status}) ->
    {false, Status}.

verify_message(#{type := continue, token := Token} = Message, State) ->
    %% reject continuations with a bad token
    case is_valid_token(State, {continue, Token}) of
        true ->
            {ok, Message, State};
        false ->
            {error, #{error => bad_token}, State}
    end;
verify_message(Message, State) ->
    %% should we even accept the message?
    %% at a minimum:
    %%   - we must be ready to accept
    %%   - we must support vsn, if specified
    %%   - any dependencies must be met
    case ready_to_accept(Message, State) of
        {true, _} ->
            case unmet_needs(Message, State) of
                nil ->
                    callback(State, {verify_message, 2}, [Message, State], {ok, Message, State});
                {vsn, Vsn} ->
                    {incapable, Vsn, State};
                {deps, Deps} ->
                    {missing, Deps, State}
            end;
        {false, Reason} ->
            {retry, Reason, State}
    end.

write_through(#{type := move}, _, _) ->
    {0, infinity};
write_through(#{type := tether}, _, _) ->
    {0, infinity};
write_through(#{mute := true}, _, _) ->
    {0, infinity};
write_through(Message, N, State) ->
    %% how many copies of a message are required for a successful write? in what timeframe?
    callback(State, {write_through, 3}, [Message, N, State], {1, infinity}).

handle_idle(State) ->
    callback(State, {handle_idle, 1}, [State], fun () -> sleep(State) end).

handle_info(Info, State) ->
    %% handle all other messages received by the listener (or worker via the listener)
    %% by default, if the loom doesn't takeover, behave as if we weren't trapping exits
    Untrap = fun () -> proc:untrap(Info, State) end,
    callback(State, {handle_info, 2}, [Info, State], Untrap).

cancel_builtin(#{yarn := Yarn}, Node, false, State) when Node =:= node() ->
    %% cancel pending emit of a yarn every time we see one replayed
    %% don't do this for new messages, since they are already popped off the stack
    util:modify(State, [emits], fun (E) -> lists:keydelete({node(), Yarn}, 1, E) end);
cancel_builtin(_Message, _Node, _IsNew, State) ->
    State.

handle_builtin(#{type := upgrade, vsn := Vsn}, Node, _, State) ->
    util:modify(State, [vsns, Node], Vsn);
handle_builtin(#{type := save}, _, true, State) ->
    save(maybe_reply(ok, State));
handle_builtin(#{type := sleep}, _, true, State) ->
    sleep(maybe_reply({goodnight, moon}, State));

handle_builtin(Message = #{type := start}, Node, _, State) ->
    erloom_electorate:handle_ctrl(Message, Node, start(Node, State));
handle_builtin(Message = #{type := stop}, Node, _, State) ->
    erloom_electorate:handle_ctrl(Message, Node, stop(Node, State));

handle_builtin(Message = #{type := motion}, Node, _, State) ->
    erloom_electorate:handle_motion(Message, Node, State);
handle_builtin(Message = #{type := ballot}, Node, _, State) ->
    erloom_electorate:handle_ballot(Message, Node, State);
handle_builtin(Message = #{type := ratify}, Node, _, State) ->
    erloom_electorate:handle_ratify(Message, Node, State);

handle_builtin(Message = #{type := move}, Node, _, State) when Node =:= node() ->
    %% its convenient to be able to push a node to make a motion
    make_motion(Message, State);
handle_builtin(Message = #{type := tether}, Node, _, State) when Node =:= node() ->
    %% its convenient to be able to push a node to move a chain
    make_tether(Message, State);

handle_builtin(Message = #{type := command}, Node, _, State) ->
    %% commands apply generic operations to a path in the state
    erloom_commands:handle_command(Message, Node, State);
handle_builtin(Message = #{type := task}, Node, _, State) ->
    %% task messages are for the surety to either retry or complete a task
    erloom_surety:handle_task(Message, Node, State);

handle_builtin(Message = #{type := continue, token := Token}, Node, IsNew, State) ->
    %% calls handle_message with a tuple, which never happens elsewhere
    %% this allows handlers to handle continuations inline, but affects the spec
    Frame = util:lookup(State, [tokens, {continue, Token}, frame]),
    handle_message({util:get(Message, param), Frame}, Node, IsNew, State);

handle_builtin(_Message, _Node, _IsNew, State) ->
    State#{response => ok}.

handle_message(Message, Node, IsNew, State) ->
    %% called to transform state for every message on every node
    %% happens 'at least once', should be externally idempotent
    %% for 'at most once', check if is new
    State1 = cancel_builtin(Message, Node, IsNew, State),
    State2 = handle_builtin(Message, Node, IsNew, State1),
    State3 = callback(State2, {handle_message, 4}, [Message, Node, IsNew, State2], State2),
    maybe_reply(State3).

command_called(Command, Node, DidChange, State) ->
    callback(State, {command_called, 4}, [Command, Node, DidChange, State], State).

vote_on_motion(Motion, Mover, State) ->
    callback(State, {vote_on_motion, 3}, [Motion, Mover, State], {{yea, ok}, State}).

motion_decided(Motion, Mover, Decision, State) ->
    %% in general one should suture (not stitch) from 'motion_decided'
    %% otherwise replay of emit could occur before the decision, causing re-emit
    callback(State, {motion_decided, 4}, [Motion, Mover, Decision, State], State).

task_completed(Message = #{name := config}, Node, Result, State) ->
    State1 = erloom_electorate:task_completed(Message, Node, Result, State),
    callback(State1, {task_completed, 4}, [Message, Node, Result, State1], State1);
task_completed(Message = #{yarn := Yarn}, Node, Result, State) ->
    %% let the final task value serve as a potential response for the yarn
    %% NB: there is no way to override this from the task_completed handler
    %%     the response in the handler is for the task process itself
    State1 = maybe_reply(Yarn, util:get(Message, value), State),
    callback(State1, {task_completed, 4}, [Message, Node, Result, State1], State1).

task_continued(Name, Reason, Clock, Arg, State) ->
    callback(State, {task_continued, 5}, [Name, Reason, Clock, Arg, State], ok).

check_node(Node, Unanswered, State) ->
    callback(State, {check_node, 3}, [Node, Unanswered, State], State).

needs_upgrade(Vsn, State) ->
    callback(State, {needs_upgrade, 2}, [Vsn, State], State).

callback(#{spec := Spec}, FA, Args) ->
    callback(Spec, FA, Args);
callback(Mod, {Fun, _}, Args) when is_atom(Mod) ->
    erlang:apply(Mod, Fun, Args);
callback(Spec, {Fun, Arity}, Args) when is_tuple(Spec) ->
    callback(element(1, Spec), {Fun, Arity}, Args).

callback(#{spec := Spec}, FA, Args, Default) ->
    callback(Spec, FA, Args, Default);
callback(Mod, {Fun, Arity}, Args, Default) when is_atom(Mod) ->
    case erlang:function_exported(Mod, Fun, Arity) of
        true ->
            erlang:apply(Mod, Fun, Args);
        false ->
            case code:is_loaded(Mod) of
                {file, _} when is_function(Default) ->
                    Default();
                {file, _} ->
                    Default;
                false ->
                    {module, Mod} = code:ensure_loaded(Mod),
                    callback(Mod, {Fun, Arity}, Args, Default)
            end
    end;
callback(Spec, {Fun, Arity}, Args, Default) when is_tuple(Spec) ->
    callback(element(1, Spec), {Fun, Arity}, Args, Default).

change_peers({'+', Nodes}, State) ->
    util:accrue(State, [peers], [{addnew, util:mapped(Nodes)}, {except, [node()]}]);
change_peers({'-', Nodes}, State) ->
    util:accrue(State, [peers], [{except, Nodes}]);
change_peers({'=', Nodes}, State) ->
    change_peers({'+', Nodes}, State#{peers => #{}}).

%% Emit a message to be handled next, before any other messages are received
%% Typically only do this with muted or new messages, otherwise happens every replay
%% Same goes below for switching the message (which also transfers the reply)

emit(Message, State) ->
    util:modify(State, [emits], fun (E) -> [{undefined, Message, undefined}|E] end).

%% Effectively switch the current message with another one
%% Transfers the reply fun to a new emit at the front of the stack

switch_message(Message, State) ->
    {Reply, State1} = eject_reply(State),
    util:modify(State1, [emits], fun (E) -> [{undefined, Message, Reply}|E] end).

switch_unmuted(Message, State) ->
    switch_message(util:delete(Message, mute), State).

%% Yarns serve 3 purposes:
%%  1. Deferring replies to a message
%%  2. Generating new information, and communicating it to the group
%%  3. Preventing the duplicate generation of new information

stitch_yarn(Message = #{yarn := Yarn}, State) ->
    %% add to emits: stack yarns per node
    %% yarns always defer replies (i.e. we should eventually reply, now or later)
    %% if the yarn is specified, assume deps are too
    State1 = defer_reply(Yarn, State),
    util:modify(State1, [emits], fun (E) -> E ++ [{{node(), Yarn}, Message}] end);
stitch_yarn(Message, State = #{message := #{yarn := Yarn}}) ->
    %% thread with the yarn of the current message, if there is one that has one
    %% also add minimal deps on current message
    stitch_yarn(min_deps(Message#{yarn => Yarn}, State), State);
stitch_yarn(Message, State) ->
    %% otherwise thread with a yarn based on the current locus
    %% also add minimal deps on current message
    %% NB: don't rely on the default name if creating multiple yarns
    stitch_yarn(min_deps(Message#{yarn => after_locus(State)}, State), State).

suture_yarn(Message = #{yarn := _}, State) ->
    %% emit a message that depends on the entire current context
    stitch_yarn(max_deps(Message, State), State);
suture_yarn(Message, State = #{message := #{yarn := Yarn}}) ->
    %% by default thread with the yarn of the current message
    suture_yarn(Message#{yarn => Yarn}, State).

%% Tasks have similar "defer until tip" semantics as emit messages
%% however processes must be restored on wake, making them more complicated

stitch_task(Base = #{name := _, yarn := Yarn}, Node, Task, State) ->
    %% if its a base message, assume deps and yarn are set already
    State1 = defer_reply(Yarn, State),
    erloom_surety:enqueue_task(Base, Node, Task, State1);
stitch_task(Name, Node, Task, State = #{message := #{yarn := Yarn}}) ->
    %% thread with the yarn of the current message, if there is one that has one
    %% also add minimal deps on current message
    Base = min_deps(#{name => Name, yarn => Yarn}, State),
    stitch_task(Base, Node, Task, State);
stitch_task(Name, Node, Task, State) ->
    %% otherwise thread with a yarn based on the current locus
    %% also add minimal deps on current message
    Base = min_deps(#{name => Name, yarn => after_locus(State)}, State),
    stitch_task(Base, Node, Task, State).

suture_task(Base = #{name := _}, Node, Task, State) ->
    %% task messages will depend on the entire current context, and be threaded
    %% useful if trigger is not just the current message but a set of messages
    stitch_task(max_deps(Base, State), Node, Task, State);
suture_task(Name, Node, Task, State = #{message := #{yarn := Yarn}}) ->
    suture_task(#{name => Name, yarn => Yarn}, Node, Task, State);
suture_task(Name, Node, Task, State) ->
    suture_task(#{name => Name, yarn => after_locus(State)}, Node, Task, State).

make_motion(Motion, State) ->
    stitch_yarn(erloom_electorate:motion(Motion, State), State).

make_tether(Change, State) ->
    stitch_yarn(erloom_electorate:tether(Change, State), State).

%% continuations
%%
%% the continuation token is left in the state and can be handed out externally
%% if the loom receives a continue message with the token (and an optional param),
%% the {Param, Frame} will be used to call handle_message

create_continuation(Frame, State) ->
    create_continuation(Frame, {3, days}, State).

create_continuation(Frame, LifeTime, State) ->
    %% the frame gives the context for the continuation
    %% the continuation token can be retrieved from the state
    Token = base64url:encode(crypto:rand_bytes(48)),
    Create = #{
      type => command,
      verb => create,
      path => [tokens, {continue, Token}],
      value => #{
        expiration => time:unix(time:pass(LifeTime)),
        frame => Frame
       }
     },
    loom:emit(Create, State#{continuation => Token}).

obtain_continuation(Frame, State) ->
    obtain_continuation(Frame, {{2, days}, {3, days}}, State).

obtain_continuation(Frame, {MinLifeTime, MaxLifeTime}, State) ->
    Exp = time:unix(time:pass(MinLifeTime)),
    Fun =
        fun ({{continue, T}, #{frame := F, expiration := E}}, _) when F =:= Frame, E > Exp ->
                T;
            (_, A) ->
                A
        end,
    case util:fold(Fun, undefined, util:get(State, tokens)) of
        undefined ->
            create_continuation(Frame, MaxLifeTime, State);
        Token ->
            State#{continuation => Token}
    end.

%% token management
%%
%% tokens are just secrets kept by the loom and associated with a value
%% tokens can expire, they can be used e.g. to ensure a client is allowed to connect
%% looms may use tokens for shared secrets in other types of messages
%% internally they are used to enable continuations

is_valid_token(State, Token) ->
    Now = time:unix(),
    case util:lookup(State, [tokens, Token]) of
        undefined ->
            false;
        #{expiration := Exp} when Exp < Now ->
            false;
        #{} ->
            true
    end.

create_token(State, Token, Value = #{}) ->
    util:modify(State, [tokens, Token], Value).

forget_token(State, Token) ->
    util:remove(State, [tokens, Token]).

purge_tokens(State = #{tokens := Tokens}) ->
    Now = time:unix(),
    Tokens1 =
        util:fold(fun ({_, #{expiration := Exp}}, Acc) when Exp < Now ->
                          Acc;
                      ({T, V}, Acc) ->
                          util:set(Acc, T, V)
                  end, #{}, Tokens),
    State#{tokens => Tokens1}.
