-module(erloom_bin).
-author("Jared Flatow").

-export([locus_formatter/1,
         message_formatter/1]).

locus_formatter(_Opts) ->
    fun ({{Node, IId}, {{R, A}, {R, B}}}) ->
            io_lib:format("~s ~s ~s ~8B ~8B", [Node, IId, R, A, B])
    end.

message_formatter(Opts) ->
    message_formatter(Opts, [{deps, skip},
                             {refs, skip},
                             {time, {unix, at}},
                             {type, true},
                             {kind, true},
                             {yarn, skip}]).

message_formatter(Opts, Defaults) ->
    {Desc, Transforms} =
        util:fold(fun (Opt, {F, Ts}) ->
                          case message_format(Opt) of
                              {skip, Pop} ->
                                  {F, [{Pop, skip}|Ts]};
                              {Fmt, Pop} ->
                                  {[Fmt|F], [{Pop, nil}|Ts]};
                              {Fmt, Pop, Fun} ->
                                  {[Fmt|F], [{Pop, Fun}|Ts]}
                          end
                  end, {[], []}, util:update(Defaults, Opts)),
    Format = lists:flatten(str:join(lists:reverse(Desc), "\t")),
    fun (Message) ->
            {Vals, Message1} =
                util:fold(fun ({{Field, Default}, Fun}, {Vs, M}) ->
                                  case util:pop(M, Field, Default) of
                                      {_, M1} when Fun =:= skip ->
                                          {Vs, M1};
                                      {V, M1} when is_function(Fun) ->
                                          {[Fun(V)|Vs], M1};
                                      {V, M1} ->
                                          {[V|Vs], M1}
                                  end
                          end, {[], Message}, Transforms),
            [io_lib:format(Format, Vals), "\t", line(Message1)]
    end.

message_format({deps, true}) ->
    {"~2B", {deps, #{}}, fun map_size/1};
message_format({refs, true}) ->
    {"~2B", {refs, []}, fun erloom:loci_count/1};
message_format({type, true}) ->
    {"~-8s", {type, "-"}};
message_format({kind, true}) ->
    {"~-8s", {kind, "-"}};
message_format({yarn, true}) ->
    {"~-48s", {yarn, "-"}, fun spin/1};
message_format({time, {unix, Field}}) ->
    {"~-19s", {Field, undefined}, fun timestamp/1};
message_format({Field, skip}) ->
    {skip, {Field, undefined}}.

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

timestamp(undefined) ->
    "-";
timestamp(DateTime) ->
    time:stamp(calendar:universal_time_to_local_time(time:datetime({unix, DateTime}))).
