%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_lib).

-export([
         ceiling/1,
         default/2,
         lazy_default/2,
         dump/1,
         dump/2,
         id/1,
         ignore/1,
         ignore/2,
         % maybe
         iter_maybe/2,
         % cohercion
         to_list/1,
         to_binary/1,
         to_string/1,
         to_atom/1,
         ra_server_id_to_local_name/1,
         ra_server_id_node/1,
         update_element/3,
         zpad_hex/1,
         zpad_filename/3,
         zpad_filename_incr/1,
         zpad_extract_num/1,
         zpad_upgrade/3,
         recursive_delete/1,
         make_uid/0,
         make_uid/1,
         make_dir/1,
         derive_safe_string/2,
         validate_base64uri/1,
         partition_parallel/2,
         partition_parallel/3,
         retry/2,
         retry/3,
         write_file/2,
         write_file/3,
         sync_file/1,
         lists_chunk/2,
         lists_detect_sort/1,
         lists_shuffle/1,
         is_dir/1,
         is_file/1,
         ensure_dir/1,
         consult/1,
         cons/2
        ]).

-type file_err() :: file:posix() | badarg | terminated | system_limit.

-export_type([file_err/0]).

-include_lib("kernel/include/file.hrl").

ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X-T =:= 0 of
      true -> T;
      false -> T + 1
    end.

default(undefined, Def) ->
    Def;
default(Value, _Def) ->
    Value.

-spec lazy_default(undefined | term(), fun (() -> term())) -> term().
lazy_default(undefined, DefGen) ->
    DefGen();
lazy_default(Value, _DefGen) ->
    Value.

dump(Term) ->
    dump("Dump", Term).

dump(Prefix, Term) ->
    io:format("~p: ~p", [Prefix, Term]),
    Term.

id(X) -> X.

ignore(_X) -> ok.
ignore(_X, _Y) -> ok.


-spec iter_maybe(undefined | term(), fun()) -> ok.
iter_maybe(undefined, _F) ->
    ok;
iter_maybe(M, F) ->
    _ = F(M),
    ok.

-spec to_list(atom() | binary() | list() | integer()) -> list().
to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(I) when is_integer(I) ->
    integer_to_list(I);
to_list(L) when is_list(L) ->
    L.

-spec to_binary(atom() | binary() | list() | integer()) -> binary().
to_binary(B) when is_binary(B) ->
    B;
to_binary(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
to_binary(I) when is_integer(I) ->
    integer_to_binary(I);
to_binary(L) when is_list(L) ->
    list_to_binary(L).

-spec to_string(binary() | string()) -> string().
to_string(B) when is_binary(B) ->
    binary_to_list(B);
to_string(L) when is_list(L) ->
    L.

-spec to_atom(atom() | list() | binary()) -> atom().
to_atom(A) when is_atom(A) ->
    A;
to_atom(B) when is_binary(B) ->
    list_to_atom(binary_to_list(B));
to_atom(L) when is_list(L) ->
    list_to_atom(L).

ra_server_id_to_local_name({Name, _}) -> Name;
ra_server_id_to_local_name(Name) when is_atom(Name) -> Name.

ra_server_id_node({_Name, Node}) -> Node;
ra_server_id_node(Name) when is_atom(Name) -> node().

update_element(Index, T, Update) when is_tuple(T) ->
    setelement(Index, T, Update(element(Index, T))).

zpad_hex(Num) ->
    lists:flatten(io_lib:format("~16.16.0B", [Num])).

zpad_filename("", Ext, Num) ->
    lists:flatten(io_lib:format("~16..0B.~ts", [Num, Ext]));
zpad_filename(Prefix, Ext, Num) ->
    lists:flatten(io_lib:format("~ts_~16..0B.~ts", [Prefix, Num, Ext])).

zpad_filename_incr(Fn) ->
    Base = filename:basename(Fn),
    Dir = filename:dirname(Fn),
    case re:run(Base, "(.*)([0-9]{16})(.*)",
                [{capture, all_but_first, list}]) of
        {match, [Prefix, NumStr, Ext]} ->
            Num = list_to_integer(NumStr),
            NewFn = lists:flatten(io_lib:format("~ts~16..0B~ts",
                                                [Prefix, Num + 1, Ext])),
            filename:join(Dir, NewFn);
        _ ->
            undefined
    end.

zpad_extract_num(Fn) ->
    {match, [_, NumStr, _]} = re:run(Fn, "(.*)([0-9]{16})(.*)",
                                     [{capture, all_but_first, list}]),
    list_to_integer(NumStr).

zpad_upgrade(Dir, File, Ext) ->
    B = filename:basename(File, Ext),
    case length(B) of
        8 ->
            %% old format, convert and rename
            F = "00000000" ++ B ++ Ext,
            New = filename:join(Dir, F),
            Old = filename:join(Dir, File),
            ok = file:rename(Old, New),
            F;
        16 ->
            File
    end.


recursive_delete(Dir) ->
    case is_dir(Dir) of
        true ->
            case prim_file:list_dir(Dir) of
                {ok, Files} ->
                    Fun =
                    fun(F) -> recursive_delete(filename:join([Dir, F])) end,
                    lists:foreach(Fun, Files),
                    delete(Dir, directory);
                {error, enoent} ->
                    ok;
                {error, Reason} ->
                    Text = file:format_error(Reason),
                    throw_error("delete file ~ts: ~ts\n", [Dir, Text])
            end;
        false ->
            delete(Dir, regular)
    end.

delete(File, Type) ->
    case do_delete(File, Type) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            Text = file:format_error(Reason),
            throw_error("delete file ~ts: ~ts\n", [File, Text])
    end.

do_delete(File, regular) ->
    prim_file:delete(File);
do_delete(Dir, directory) ->
    prim_file:del_dir(Dir).

-spec throw_error(string(), list()) -> no_return().
throw_error(Format, Args) ->
    throw({error, lists:flatten(io_lib:format(Format, Args))}).

%% "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
-define(GENERATED_UID_CHARS,
        {65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78,
         79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 48, 49,
         50, 51, 52, 53, 54, 55, 56, 57}).

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-=").
-define(UID_LENGTH, 12).

-spec make_uid() -> binary().
make_uid() ->
    make_uid(<<>>).

-spec make_uid(atom() | binary() | string()) -> binary().
make_uid(Prefix0) ->
    ChrsSize = size(?GENERATED_UID_CHARS),
    F = fun(_, R) ->
                [element(rand:uniform(ChrsSize), ?GENERATED_UID_CHARS) | R]
        end,
    Prefix = to_binary(Prefix0),
    B = list_to_binary(lists:foldl(F, "", lists:seq(1, ?UID_LENGTH))),
    <<Prefix/binary, B/binary>>.

-spec make_dir(file:name_all()) ->
    ok | {error, file:posix() | badarg}.
make_dir(Dir) ->
    case is_dir(Dir) of
        true -> ok;
        false ->
            handle_ensure_dir(ensure_dir(Dir), Dir)
    end.

handle_ensure_dir(ok, Dir) ->
    handle_make_dir(prim_file:make_dir(Dir));
handle_ensure_dir(Error, _Dir) ->
    Error.

handle_make_dir(ok) ->
    ok;
handle_make_dir({error, eexist}) ->
    ok;
handle_make_dir(Error) ->
    Error.

-spec validate_base64uri(unicode:chardata()) -> boolean().
validate_base64uri(Str) ->
    catch
    begin
        [begin
             case lists:member(C, ?BASE64_URI_CHARS) of
                 true -> ok;
                 false -> throw(false)
             end
         end || C <- string:to_graphemes(Str)],
        string:is_empty(Str) == false
    end.


derive_safe_string(S, Num) ->
    F = fun Take([], Acc) ->
                string:reverse(Acc);
            Take([G | Rem], Acc) ->
                case lists:member(G, ?BASE64_URI_CHARS) of
                    true ->
                        Take(string:next_grapheme(Rem), [G | Acc]);
                    false ->
                        Take(string:next_grapheme(Rem), Acc)
                end
         end,
     string:slice(F(string:next_grapheme(S), []), 0, Num).

-spec partition_parallel(fun((any()) -> boolean()), [any()]) ->
    {ok, [any()], [any()]} | {error, any()}.
partition_parallel(F, Es) ->
    partition_parallel(F, Es, 60000).

-spec partition_parallel(fun((any()) -> boolean()), [any()], timeout()) ->
    {ok, [any()], [any()]} | {error, any()}.
partition_parallel(F, Es, Timeout) ->
    Parent = self(),
    Running = [{spawn_monitor(fun() ->
                                      Parent ! {self(), F(E)}
                              end), E}
               || E <- Es],
    case collect(Running, {[], []}, Timeout) of
        {error, _} = E -> E;
        {Successes, Failures} -> {ok, Successes, Failures}
    end.

collect([], Acc, _Timeout) ->
    Acc;
collect([{{Pid, MRef}, E} | Next], {Left, Right}, Timeout) ->
    receive
        {Pid, true} ->
            erlang:demonitor(MRef, [flush]),
            collect(Next, {[E | Left], Right}, Timeout);
        {Pid, false} ->
            erlang:demonitor(MRef, [flush]),
            collect(Next, {Left, [E | Right]}, Timeout);
        {'DOWN', MRef, process, Pid, Reason} ->
            collect(Next, {Left, [{E, Reason} | Right]}, Timeout)
    after Timeout ->
        {error, {partition_parallel_timeout, Left, Right}}
    end.

retry(Func, Attempts) ->
    retry(Func, Attempts, 5000).

retry(_Func, 0, _Sleep) ->
    exhausted;
retry(Func, Attempt, Sleep) ->
    % do not retry immediately
    case catch Func() of
        ok ->
            ok;
        true ->
            ok;
        _ ->
            timer:sleep(Sleep),
            retry(Func, Attempt - 1)
    end.

-spec write_file(file:name_all(), iodata()) ->
    ok | {error, file_err()}.
write_file(Name, IOData) ->
    write_file(Name, IOData, true).

-spec write_file(file:name_all(), iodata(), Sync :: boolean()) ->
    ok | {error, file_err()}.
write_file(Name, IOData, Sync) ->
    case file:open(Name, [binary, write, raw]) of
        {ok, Fd} ->
            case file:write(Fd, IOData) of
                ok ->
                    case Sync of
                        true ->
                            sync_and_close_fd(Fd);
                        false ->
                            ok
                    end;
                Err ->
                    _ = file:close(Fd),
                    Err
            end;
        Err ->
            Err
    end.

-spec sync_file(file:name_all()) ->
    ok | {error, file_err()}.
sync_file(Name) ->
    case file:open(Name, [binary, read, write, raw]) of
        {ok, Fd} ->
            sync_and_close_fd(Fd);
        Err ->
            Err
    end.

-spec sync_and_close_fd(file:fd()) ->
    ok | {error, file_err()}.
sync_and_close_fd(Fd) ->
    case ra_file:sync(Fd) of
        ok ->
            file:close(Fd);
        Err ->
            _ = file:close(Fd),
            Err
    end.

lists_chunk(0, List) ->
    error(invalid_size, [0, List]);
lists_chunk(Size, List) ->
    lists_chunk(Size, List, []).

lists_chunk(_Size, [], Acc)  ->
    lists:reverse(Acc);
lists_chunk(Size, List, Acc) when length(List) < Size ->
    lists:reverse([List | Acc]);
lists_chunk(Size, List, Acc) ->
    {L, Rem} = lists_take(Size, List, []),
    lists_chunk(Size, Rem, [L | Acc]).

lists_take(0, List, Acc) ->
    {lists:reverse(Acc), List};
lists_take(_N, [], Acc) ->
    {lists:reverse(Acc), []};
lists_take(N, [H | T], Acc) ->
    lists_take(N-1, T, [H | Acc]).

lists_detect_sort([]) ->
    undefined;
lists_detect_sort([_]) ->
    undefined;
lists_detect_sort([A | [A | _] = Rem]) ->
    %% direction not determined yet
    lists_detect_sort(Rem);
lists_detect_sort([A, B | Rem]) when A > B ->
    do_descending(B, Rem);
lists_detect_sort([A, B | Rem]) when A < B ->
    do_ascending(B, Rem).

do_descending(_A, []) ->
    descending;
do_descending(A, [B | Rem])
  when B =< A ->
    do_descending(B, Rem);
do_descending(_A, _) ->
    unsorted.

do_ascending(_A, []) ->
    ascending;
do_ascending(A, [B | Rem])
  when B >= A ->
    do_ascending(B, Rem);
do_ascending(_A, _) ->
    unsorted.

%% Reorder a list randomly.
-spec lists_shuffle(list()) -> list().
lists_shuffle(List0) ->
    List1 = [{rand:uniform(), Elem} || Elem <- List0],
    [Elem || {_, Elem} <- lists:keysort(1, List1)].

is_dir(Dir) ->
    case prim_file:read_file_info(Dir) of
        {ok, #file_info{type = directory}} ->
            true;
        _ ->
            false
    end.

is_file(File) ->
    case prim_file:read_file_info(File) of
        {ok, #file_info{type = regular}} ->
            true;
        _ ->
            false
    end.


-spec consult(file:filename()) ->
    {ok, term()} | {error, term()}.
consult(Path) ->
    case prim_file:read_file(Path) of
        {ok, Data} ->
            Str = erlang:binary_to_list(Data),
            tokens(Str);
        Err ->
            Err
    end.

cons(Item, List)
  when is_list(List) ->
    [Item | List].

tokens(Str) ->
    case erl_scan:string(Str) of
        {ok, Tokens, _EndLoc} ->
            erl_parse:parse_term(Tokens);
        {error, Err, _ErrLoc} ->
            {error, Err}
    end.


%% raw copy of ensure_dir
ensure_dir("/") ->
    ok;
ensure_dir(F) ->
    Dir = filename:dirname(F),
    case is_dir(Dir) of
        true ->
            ok;
        false when Dir =:= F ->
            %% Protect against infinite loop
            {error, einval};
        false ->
            _ = ensure_dir(Dir),
            case prim_file:make_dir(Dir) of
                {error, eexist} = EExist ->
                    case is_dir(Dir) of
                        true ->
                            ok;
                        false ->
                            EExist
                    end;
                Err ->
                    Err
            end
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

lists_chunk_test() ->
    ?assertError(invalid_size, lists_chunk(0, [a])),
    ?assertMatch([], lists_chunk(2, [])),
    ?assertMatch([[a]], lists_chunk(2, [a])),
    ?assertMatch([[a, b]], lists_chunk(2, [a, b])),
    ?assertMatch([[a, b], [c]], lists_chunk(2, [a, b, c])),
    ?assertMatch([[a, b], [c, d]], lists_chunk(2, [a, b, c, d])),
    ok.

make_uid_test() ->
    U1 = make_uid(),
    U2 = make_uid(),
    ?debugFmt("U1 ~ts U2 ~s", [U1, U2]),
    ?assertNotEqual(U1, U2),
    <<"ABCD", _/binary>> = make_uid("ABCD"),
    <<"ABCD", _/binary>> = make_uid(<<"ABCD">>),
    ok.

zpad_filename_incr_test() ->
    Fn = "/lib/blah/prefix_0000000000000001.segment",
    Ex = "/lib/blah/prefix_0000000000000002.segment",
    Ex = zpad_filename_incr(Fn),
    undefined = zpad_filename_incr("000000000000001"),
    ok.

zpad_filename_incr_utf8_test() ->
    Fn = "/lib/🐰/prefix/0000000000000001.segment",
    Ex = "/lib/🐰/prefix/0000000000000002.segment",
    Ex = zpad_filename_incr(Fn),
    undefined = zpad_filename_incr("000000000000001"),
    ok.

derive_safe_string_test() ->
    S = <<"bønana"/utf8>>,
    S2 = "bønana",
    [] = derive_safe_string(<<"">>, 4),
    "bnan" = derive_safe_string(S, 4),
    "bnan" = derive_safe_string(S2, 4),
    ok.

validate_base64uri_test() ->
    false = validate_base64uri(""), %% false
    true = validate_base64uri(?BASE64_URI_CHARS),
    false = validate_base64uri("asdføasdf"),
    false = validate_base64uri("asdf/asdf"),
    ok.

zeropad_test() ->
    "0000000000000037" = zpad_hex(55),
    ok.

lists_detect_sort_test() ->
    ?assertEqual(undefined, lists_detect_sort([])),
    ?assertEqual(undefined, lists_detect_sort([1])),
    ?assertEqual(undefined, lists_detect_sort([1, 1])),
    ?assertEqual(ascending, lists_detect_sort([1, 1, 2])),
    ?assertEqual(unsorted, lists_detect_sort([1, 2, 1])),
    ?assertEqual(unsorted, lists_detect_sort([2, 1, 2])),
    ?assertEqual(descending, lists_detect_sort([2, 1])),
    ?assertEqual(descending, lists_detect_sort([2, 2, 1])),
    ?assertEqual(ascending, lists_detect_sort([1, 1, 2])),
    ?assertEqual(ascending, lists_detect_sort([1, 2, 3, 4, 6, 6])),
    ?assertEqual(unsorted, lists_detect_sort([1, 2, 3, 4, 6, 5])),

    ok.

partition_parallel_test() ->
    ?assertMatch({error, {partition_parallel_timeout, [], []}},
                 partition_parallel(fun(_) ->
                                        timer:sleep(infinity)
                                    end, [1, 2, 3], 1000)),
    ok.

-endif.
