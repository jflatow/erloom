-module(erloom).
-author("Jared Flatow").

-export_type([logid/0,
              mark/0,
              which/0,
              range/0,
              delta/0,
              edge/0,
              edges/0,
              entries/0,
              locus/0,
              loci/0,
              logs/0]).

-type logid() :: binary().
-type mark() :: {logid(), log:mark()}.
-type which() :: {node(), logid()}.
-type range() :: {mark() | undefined, mark()}.
-type delta() :: #{node() => range()}.
-type edge() :: #{node() => mark()}.
-type edges() :: #{node() => edge()}.
-type entries() :: #{which() => [log:entry()]}.
-type locus() :: {which(), log:range()}.
-type loci() :: locus() | [locus()].
-type logs() :: #{which() => log:log()}.

-spec delta_lower(delta()) -> edge().
-spec delta_upper(delta()) -> edge().
-spec edge_delta(edge(), edge()) -> delta().
-spec edge_hull(edge(), edge()) -> edge().
-spec locus_node(locus()) -> node().
-spec locus_before(locus()) -> edge().
-spec locus_after(locus()) -> edge().
-spec loci_before(loci()) -> edge().
-spec loci_after(loci()) -> edge().

-behavior(application).
-export([start/0, stop/0]).
-export([start/2, stop/1]).

-behavior(supervisor).
-export([init/1]).
-define(MaxRestarts, 10).
-define(MaxRestartWindow, 10).
-define(Child(Mod, Args, Type), {Mod, {Mod, start, Args}, permanent, 5000, Type, [Mod]}).
-define(Children, [?Child(erloom_registry, [], worker)]).

-export([delta_bound/2,
         delta_lower/1,
         delta_upper/1,
         edge_delta/2,
         edge_delta/3,
         edge_hull/2,
         edge_hull/3,
         edges_max/2,
         locus_node/1,
         locus_before/1,
         locus_after/1,
         loci_before/1,
         loci_after/1,
         loci_count/1]).

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

delta_bound(D, forward) ->
    delta_upper(D);
delta_bound(D, reverse) ->
    delta_lower(D).

delta_lower(D) ->
    util:map(D, fun ({L, _}) -> L end).

delta_upper(D) ->
    util:map(D, fun ({_, U}) -> U end).

edge_delta(X, Y) ->
    edge_delta(X, Y, forward).

edge_delta(X, Y, Dir) ->
    maps:fold(fun (Node, XMark, Delta) ->
                      case maps:get(Node, Y, undefined) of
                          undefined when Dir =:= forward ->
                              Delta#{Node => {undefined, XMark}};
                          undefined when Dir =:= reverse ->
                              Delta#{Node => {XMark, undefined}};
                          YMark when YMark < XMark, Dir =:= forward ->
                              Delta#{Node => {YMark, XMark}};
                          YMark when XMark < YMark, Dir =:= reverse ->
                              Delta#{Node => {XMark, YMark}};
                          _ ->
                              Delta
                      end
              end, #{}, X).

edge_hull(X, Y) ->
    edge_hull(X, Y, forward).

edge_hull(X, Y, Dir) ->
    maps:fold(fun (Node, XMark, Max) ->
                      case maps:get(Node, Y, undefined) of
                          undefined ->
                              Max#{Node => XMark};
                          YMark when YMark < XMark, Dir =:= forward ->
                              Max#{Node => XMark};
                          YMark when XMark < YMark, Dir =:= reverse ->
                              Max#{Node => XMark};
                          _ ->
                              Max
                      end
              end, Y, X).

edges_max(OfNode, Edges) ->
    maps:fold(fun (Node, Edge, {N, M}) ->
                      case maps:get(OfNode, Edge, undefined) of
                          Mark when Mark >= M ->
                              {Node, Mark};
                          _ ->
                              {N, M}
                      end
              end, {undefined, undefined}, Edges).

locus_node({{Node, _}, _}) ->
    Node.

locus_before({{Node, IId}, {Before, _}}) ->
    #{Node => {IId, Before}}.

locus_after({{Node, IId}, {_, After}}) ->
    #{Node => {IId, After}}.

loci_before(Loci) when is_list(Loci) ->
    lists:foldl(fun (L, A) -> edge_hull(locus_before(L), A) end, #{}, Loci);
loci_before(Locus) ->
    locus_before(Locus).

loci_after(Loci) when is_list(Loci) ->
    lists:foldl(fun (L, A) -> edge_hull(locus_after(L), A) end, #{}, Loci);
loci_after(Locus) ->
    locus_after(Locus).

loci_count(List) when is_list(List) ->
    length(List);
loci_count(_) ->
    1.
