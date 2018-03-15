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
         to_atom/1,
         ra_node_id_to_local_name/1,
         ra_node_id_node/1,
         update_element/3,
         zpad_filename/3,
         zpad_filename_incr/1,
         zpad_extract_num/1,
         recursive_delete/1
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

zpad_filename_incr_test() ->
    Fn = "/lib/blah/prefix_00000001.segment",
    Ex = "/lib/blah/prefix_00000002.segment",
    Ex = zpad_filename_incr(Fn),
    undefined = zpad_filename_incr("0000001"),
    ok.

-endif.
