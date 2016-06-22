-module(erloom_registry).
-author("Jared Flatow").

%% The registry is started when erloom is run as an application.
%% This is the default usage, and by default looms use it to find or spawn pids.
%% Ids that are atoms use the builtin erlang registry and do not require the gen_server.
%% However, the default implementation of proc for looms do not use atom ids.

-behavior(gen_server).
-export([start/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([proc/2]).
-record(state, {looms}).

%% gen_server

start() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{looms=#{}}}.

handle_call(state, _From, State) ->
    {reply, State, State};
handle_call({proc, Id, Spec}, _From, #state{looms=Looms} = State) ->
    case maps:find(Id, Looms) of
        {ok, Pid} ->
            {reply, Pid, State};
        error ->
            case erloom_listener:spawn(Spec) of
                Pid when is_pid(Pid) ->
                    {reply, Pid, store_(Id, Pid, State)}
            end
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State) ->
    {noreply, erase_(Pid, State)}.

terminate(_Reason, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% state helpers

store_(Id, Pid, State = #state{looms=Looms}) ->
    State#state{looms=maps:put(Pid, Id, maps:put(Id, Pid, Looms))}.

erase_(IdOrPid, State = #state{looms=Looms}) ->
    case maps:find(IdOrPid, Looms) of
        {ok, PidOrId} ->
            State#state{looms=maps:without([IdOrPid, PidOrId], Looms)};
        error ->
            State
    end.

%% registry interface

proc(undefined, _) ->
    error(badarg);
proc(Id, Spec) when is_atom(Id) ->
    case whereis(Id) of
        undefined ->
            try
                Pid = erloom_listener:spawn(Spec),
                register(Id, Pid),
                unlink(Pid),
                Pid
            catch
                error:badarg ->
                    proc(Id, Spec)
            end;
        Pid ->
            Pid
    end;
proc(Id, Spec) ->
    gen_server:call(?MODULE, {proc, Id, Spec}).
