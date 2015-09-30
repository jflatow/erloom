-module(erloom_bin).

-export([format_message/1]).

format_message(Message) ->
    Deps = util:get(Message, deps, #{}),
    Refs = util:get(Message, refs, []),
    Type = util:get(Message, type, "-"),
    Kind = util:get(Message, kind, "-"),
    Yarn = util:get(Message, yarn, "-"),
    Line = line(maps:without([deps, refs, type, kind, yarn], Message)),
    io_lib:format("~2B ~2B\t~-8s\t~-8s\t~-48s\t~s",
                  [map_size(Deps), erloom:loci_count(Refs),
                   Type, Kind, spin(Yarn), Line]).

line(Message) ->
    util:join(
      maps:fold(fun (K, V, A) ->
                        [io_lib:format("~s=~1024p", [K, V])|A]
                end, [], Message), "\t").

spin(Yarn) when is_map(Yarn) ->
    util:join(
      maps:fold(fun (K, {IId, {Rel, Offs}}, A) when is_atom(K), is_binary(IId) ->
                        [io_lib:format("~s ~s ~s ~p", [K, IId, Rel, Offs])|A];
                    (K, V, A) ->
                        [io_lib:format("~p:~p", [K, V])|A]
                end, [], Yarn), "/");
spin(Yarn) ->
    Yarn.
