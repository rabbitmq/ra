-module(ra_lib).

-export([
         ceiling/1,
         default/2,
         lazy_default/2,
         dump/1,
         dump/2,
         id/1,
         % maybe
         iter_maybe/2,
         % cohercion
         to_list/1,
         to_binary/1,
         to_string/1,
         to_atom/1,
         ra_node_id_to_local_name/1,
         ra_node_id_node/1,
         update_element/3,
         zpad_filename/3,
         zpad_filename_incr/1,
         zpad_extract_num/1,
         recursive_delete/1,
         make_uid/0,
         make_uid/1,
         ensure_dir/1,
         derive_safe_string/2,
         validate_base64uri/1,
         partition_parallel/2
        ]).

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
    io:format("~p: ~p~n", [Prefix, Term]),
    Term.

id(X) -> X.


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

ra_node_id_to_local_name({Name, _}) -> Name;
ra_node_id_to_local_name(Name) when is_atom(Name) -> Name.

ra_node_id_node({_Name, Node}) -> Node;
ra_node_id_node(Name) when is_atom(Name) -> node().

update_element(Index, T, Update) when is_tuple(T) ->
    setelement(Index, T, Update(element(Index, T))).


zpad_filename("", Ext, Num) ->
    lists:flatten(io_lib:format("~8..0B.~s", [Num, Ext]));
zpad_filename(Prefix, Ext, Num) ->
    lists:flatten(io_lib:format("~s_~8..0B.~s", [Prefix, Num, Ext])).

zpad_filename_incr(Fn) ->
    case re:run(Fn, "(.*)([0-9]{8})(.*)", [{capture, all_but_first, list}]) of
        {match, [Prefix, NumStr, Ext]} ->
            Num = list_to_integer(NumStr),
            lists:flatten(io_lib:format("~s~8..0B~s", [Prefix, Num+1, Ext]));
        _ ->
            undefined
    end.

zpad_extract_num(Fn) ->
    {match, [_, NumStr, _]} = re:run(Fn, "(.*)([0-9]{8})(.*)",
                                     [{capture, all_but_first, list}]),
    list_to_integer(NumStr).

recursive_delete(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            case file:list_dir(Dir) of
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
    file:delete(File);
do_delete(Dir, directory) ->
    file:del_dir(Dir).

throw_error(Format, Args) ->
    throw({error, lists:flatten(io_lib:format(Format, Args))}).

%% "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
-define(GENERATED_UID_CHARS,
        {65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,
         84,85,86,87,88,89,90,48,49,50,51,52,53,54,55,56,57}).

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-=").
-define(UID_CHARS, "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890").
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

-spec ensure_dir(file:name_all()) ->
    ok | {error, file:posix() | badarg}.
ensure_dir(Dir) ->
    case file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> ok;
        Err ->
            Err
    end.

-spec validate_base64uri(string()) -> boolean().
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

partition_parallel(F, Es) ->
   Parent = self(),
   Running = [
              {spawn_monitor(fun() -> Parent ! {self(), F(E)} end), E}
       || E <- Es],
   collect(Running, {[], []}, 60000).

collect([], Acc, _Timeout) -> Acc;
collect([{{Pid, MRef}, E} | Next], {Left, Right}, Timeout) ->
  receive
    {Pid, true} ->
      erlang:demonitor(MRef, [flush]),
      collect(Next, {[E | Left], Right}, Timeout);
      % [{left, E} | collect(Next, Timeout)];
    {Pid, false} ->
      erlang:demonitor(MRef, [flush]),
      collect(Next, {Left, [E | Right]}, Timeout);
    {'DOWN', MRef, process, Pid, _Reason} ->
      collect(Next, {Left, [E | Right]}, Timeout)
  after Timeout ->
    exit(partition_parallel_timeout)
  end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_uid_test() ->
    U1 = make_uid(),
    U2 = make_uid(),
    ?debugFmt("U1 ~s U2 ~s", [U1, U2]),
    ?assertNotEqual(U1, U2),
    <<"ABCD", _/binary>> = make_uid("ABCD"),
    <<"ABCD", _/binary>> = make_uid(<<"ABCD">>),
    ok.

zpad_filename_incr_test() ->
    Fn = "/lib/blah/prefix_00000001.segment",
    Ex = "/lib/blah/prefix_00000002.segment",
    Ex = zpad_filename_incr(Fn),
    undefined = zpad_filename_incr("0000001"),
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

-endif.
