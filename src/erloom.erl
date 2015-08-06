-module(erloom).

-export_type([log/0,
              mark/0,
              range/0,
              delta/0,
              edge/0,
              edges/0,
              entry/0,
              entries/0]).

-opaque log() :: pid().
-opaque mark() :: {binary(), binary()} | undefined.
-type range() :: {mark(), mark()}.
-type delta() :: #{node() => range()}.
-type edge() :: #{node() => mark()}.
-type edges() :: #{node() => edge()}.
-type entry() :: {range(), binary()}.
-type entries() :: #{node() => [entry()]}.

-spec delta_lower(delta()) -> edge().
-spec delta_upper(delta()) -> edge().
-spec edge_delta(edge(), edge()) -> delta().
-spec edge_hull(edge(), edge()) -> edge().

-behavior(application).
-export([start/0, stop/0]).
-export([start/2, stop/1]).

-behavior(supervisor).
-export([init/1]).
-define(MaxRestarts, 10).
-define(MaxRestartWindow, 10).
-define(Child(Mod, Args, Type), {Mod, {Mod, start, Args}, permanent, 5000, Type, [Mod]}).
-define(Children, [?Child(erloom_registry, [], worker)]).

-export([delta_lower/1,
         delta_upper/1,
         edge_delta/2,
         edge_hull/2]).

%% command-line

start() ->
    ok = start(?MODULE).

start(App) ->
    case application:start(App) of
        {error, {not_started, Dep}} ->
            ok = start(Dep),
            ok = start(App);
        Else ->
            Else
    end.

stop() ->
    application:stop(?MODULE).

%% application

start(_StartType, _StartArgs) ->
    {ok, _} = supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

%% supervisor

init([]) ->
    {ok, {{one_for_one, ?MaxRestarts, ?MaxRestartWindow}, ?Children}}.

%% erloom

delta_lower(D) ->
    util:map(D, fun ({L, _}) -> L end).

delta_upper(D) ->
    util:map(D, fun ({_, U}) -> U end).

edge_delta(X, Y) ->
    maps:fold(fun (Node, XMark, Delta) ->
                      case maps:get(Node, Y, undefined) of
                          YMark when YMark < XMark ->
                              Delta#{Node => {YMark, XMark}};
                          _ ->
                              Delta
                      end
              end, #{}, X).

edge_hull(X, Y) ->
    maps:fold(fun (Node, XMark, Max) ->
                      case maps:get(Node, Y, undefined) of
                          YMark when YMark < XMark ->
                              Max#{Node => XMark};
                          _ ->
                              Max
                      end
              end, Y, X).
