-module(ra_log_file_meta).

-export([init/1,
         store/3,
         fetch/2,
         sync/1,
         close/1
        ]).

-include("ra.hrl").

% small fixed-size key value store for persisting no log raft data points
% replacement for dets

-type key() :: current_term | voted_for | last_applied.
-type value() :: non_neg_integer() | atom() | {atom(), atom()}.

-define(HEADER_SIZE, 8).
-define(CURRENT_TERM_OFFS, 8).
-define(CURRENT_TERM_SIZE, 8).
-define(LAST_APPLIED_OFFS, 16).
-define(LAST_APPLIED_SIZE, 8).
-define(VOTED_FOR_NAME_OFFS, 24).
-define(ATOM_SIZE, 513). % atoms max 255 characters but allow for unicode + length prefix
-define(VOTED_FOR_NODE_OFFS, ?VOTED_FOR_NAME_OFFS + ?ATOM_SIZE ).

-record(state, {fd :: file:fd()}).

-opaque state() :: #state{}.

-export_type([state/0]).

-spec init(file:filename()) -> state().
init(Fn) ->
    ok = filelib:ensure_dir(Fn),
    {ok, Fd} = file:open(Fn, [binary, raw, read, write]),
    % expand file
    {ok, _} = file:position(Fd, ?VOTED_FOR_NODE_OFFS + ?ATOM_SIZE),
    ok = file:truncate(Fd),
    % TODO: check version in header
    #state{fd = Fd}.


-spec store(key(), value(), state()) -> ok.
store(last_applied, LastApplied, #state{fd = Fd}) ->
    ok = write_integer(Fd, LastApplied, ?LAST_APPLIED_OFFS);
store(current_term, CurTerm, #state{fd = Fd}) ->
    ok = write_integer(Fd, CurTerm, ?CURRENT_TERM_OFFS);
store(voted_for, {Name, Node}, #state{fd = Fd}) ->
    ok = write_atom(Fd, Name, ?VOTED_FOR_NAME_OFFS),
    ok = write_atom(Fd, Node, ?VOTED_FOR_NODE_OFFS),
    ok;
store(voted_for, undefined, #state{fd = Fd}) ->
    % clear value
    Data = <<0:16/integer>>,
    Ops = [{?VOTED_FOR_NAME_OFFS, Data},
           {?VOTED_FOR_NODE_OFFS, Data}],
    ok = file:pwrite(Fd, Ops);
store(voted_for, Name, #state{fd = Fd}) when is_atom(Name) ->
    write_atom(Fd, Name, ?VOTED_FOR_NAME_OFFS).

-spec fetch(key(), state()) -> value() | undefined.
fetch(voted_for, #state{fd = Fd}) ->
    case read_atom(Fd, ?VOTED_FOR_NAME_OFFS) of
        undefined ->
            undefined;
        Name ->
            case read_atom(Fd, ?VOTED_FOR_NODE_OFFS) of
                undefined ->
                    Name;
                Node ->
                    {Name, Node}
            end
    end;
fetch(current_term, #state{fd = Fd}) ->
    read_integer(Fd, ?CURRENT_TERM_OFFS);
fetch(last_applied, #state{fd = Fd}) ->
    read_integer(Fd, ?LAST_APPLIED_OFFS).


-spec sync(state()) -> ok.
sync(#state{fd = Fd}) ->
    ok = file:sync(Fd).


-spec close(state()) -> ok.
close(#state{fd = Fd}) ->
    ok = file:sync(Fd),
    _ = file:close(Fd).

%%% internal


write_integer(Fd, Int, Offs) ->
    Data = <<Int:64/integer>>,
    ok = file:pwrite(Fd, Offs, Data).

read_integer(Fd, Offs) ->
    {ok, <<Int:64/integer>>} = file:pread(Fd, Offs, 8),
    Int.

write_atom(Fd, A, Offs) ->
    Ops = case atom_to_binary(A, utf8) of
              <<>> ->
                  [{Offs, <<1:1/integer, 0:15/integer>>}];
              AData ->
                  ASize = byte_size(AData),
                  [{Offs, <<0:1/integer, ASize:15/integer>>},
                   {Offs + 2, AData}]
          end,
    ok = file:pwrite(Fd, Ops).

read_atom(Fd, Offs) ->
    case file:pread(Fd, Offs, ?ATOM_SIZE) of
        {ok, <<0:1/integer, 0:15/integer,_/binary>>} -> % zero length
            undefined;
        {ok, <<1:1/integer, 0:15/integer,_/binary>>} -> % empty atom
            '';
        {ok, <<0:1/integer, Len:15/integer, Data:Len/binary, _/binary>>} ->
            % strip trailing null bytes
            binary_to_atom(Data, utf8)
    end.
