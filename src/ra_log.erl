-module(ra_log).

-include("ra.hrl").

-export([init/2,
         close/1,
         append/3,
         append_sync/3,
         fetch/2,
         fetch_term/2,
         flush/2,
         take/3,
         last/1,
         last_index_term/1,
         handle_written/2,
         last_written/1,
         next_index/1,
         read_snapshot/1,
         write_snapshot/2,
         read_meta/2,
         read_meta/3,
         write_meta/3,
         write_meta/4,
         sync_meta/1,
         exists/2
        ]).

-type ra_log_init_args() :: #{atom() => term()}.
-type ra_log_state() :: term().
-type ra_log() :: {Module :: module(), ra_log_state()}.
-type ra_meta_key() :: atom().
-type ra_log_snapshot() :: {ra_index(), ra_term(), ra_cluster(), term()}.

-export_type([ra_log_init_args/0,
              ra_log_state/0,
              ra_log/0,
              ra_meta_key/0,
              ra_log_snapshot/0
             ]).

-callback init(Args :: ra_log_init_args()) -> ra_log_state().

-callback close(State :: ra_log_state()) -> ok.

-callback append(Entry :: log_entry(),
                 overwrite | no_overwrite,
                 State :: ra_log_state()) ->
    {queued, ra_log_state()} |
    {written, ra_log_state()} |
    {error, integrity_error}.

-callback take(Start :: ra_index(), Num :: non_neg_integer(),
               State :: ra_log_state()) ->
    [log_entry()].

-callback next_index(State :: ra_log_state()) ->
    ra_index().

-callback fetch(Inx :: ra_index(), State :: ra_log_state()) ->
    maybe(log_entry()).

-callback fetch_term(Inx :: ra_index(), State :: ra_log_state()) ->
    maybe(ra_term()).

-callback handle_written(Idx :: ra_index(), Log :: ra_log_state()) -> ra_log_state().

-callback last_written(Log :: ra_log_state()) -> ra_idxterm().

-callback last(State :: ra_log_state()) ->
    maybe(log_entry()).

-callback last_index_term(State :: ra_log_state()) ->
    maybe(ra_idxterm()).

% writes snapshot to storage and discards any prior entries
-callback write_snapshot(Snapshot :: ra_log_snapshot(),
                         State :: ra_log_state()) ->
    ra_log_state().

-callback read_snapshot(State :: ra_log_state()) ->
    maybe(ra_log_snapshot()).

-callback read_meta(Key :: ra_meta_key(), State :: ra_log_state()) ->
    maybe(term()).

-callback write_meta(Key :: ra_meta_key(), Value :: term(),
                     State :: ra_log_state()) ->
    {ok, ra_log_state()} | {error, term()}.

-callback sync_meta(State :: ra_log_state()) ->
    ok.



%%
%% API
%%

-spec init(Mod::atom(), Args :: ra_log_init_args()) -> ra_log().
init(Mod, Args) ->
    {Mod, Mod:init(Args)}.

-spec close(Log :: ra_log()) -> ok.
close({Mod, Log}) ->
    Mod:close(Log).

-spec fetch(Idx::ra_index(), Log::ra_log()) -> maybe(log_entry()).
fetch(Idx, {Mod, Log}) ->
    Mod:fetch(Idx, Log).

-spec fetch_term(Idx::ra_index(), Log::ra_log()) -> maybe(ra_term()).
fetch_term(Idx, {Mod, Log}) ->
    Mod:fetch_term(Idx, Log).

-spec flush(Idx :: ra_index(), Log :: ra_log()) -> ra_log().
flush(Idx, {Mod, Log}) ->
    {Mod, Mod:flush(Idx, Log)}.

-spec append(Entry :: log_entry(),
             Overwrite :: overwrite | no_overwrite,
             State::ra_log()) ->
    {queued, ra_log()} |
    {written, ra_log()} |
    {error, integrity_error}.
append(Entry, Overwrite, {Mod, Log0}) ->
    case Mod:append(Entry, Overwrite, Log0) of
        {error, _} = Err ->
            Err;
        {Status, Log} ->
            {Status, {Mod, Log}}
    end.

append_sync({Idx, _, _} = Entry, Overwrite, Log0) ->
    case ra_log:append(Entry, Overwrite, Log0) of
        {written, Log} ->
            ra_log:handle_written(Idx, Log);
        {queued, Log} ->
            receive
                % TODO: we could now end up re-ordering written notifications
                % so need to handle that later
                {written, Idx} ->
                    ra_log:handle_written(Idx, Log)
            after 5000 ->
                      throw(ra_log_append_timeout)
            end;
        {error, _} = Err ->
            throw(Err)
    end.



-spec take(Start::ra_index(), Num::non_neg_integer(),
           State::ra_log()) -> [log_entry()].
take(Start, Num, {Mod, Log}) ->
    Mod:take(Start, Num, Log).

-spec last(State::ra_log()) -> maybe(log_entry()).
last({Mod, Log}) ->
    Mod:last(Log).

-spec last_index_term(State::ra_log()) -> maybe(ra_idxterm()).
last_index_term({Mod, Log}) ->
    Mod:last_index_term(Log).

-spec handle_written(Idx :: ra_index(), Log :: ra_log_state()) -> ra_log_state().
handle_written(Idx, {Mod, Log}) ->
    {Mod, Mod:handle_written(Idx, Log)}.

-spec last_written(Log :: ra_log_state()) -> ra_idxterm().
last_written({Mod, Log}) ->
    Mod:last_written(Log).

-spec next_index(State::ra_log()) -> ra_index().
next_index({Mod, Log}) ->
    Mod:next_index(Log).

-spec write_snapshot(Snapshot :: ra_log_snapshot(), State::ra_log()) -> ra_log().
write_snapshot(Snapshot, {Mod, Log0}) ->
    Log = Mod:write_snapshot(Snapshot, Log0),
    {Mod, Log}.

-spec read_snapshot(State::ra_log()) -> maybe(ra_log_snapshot()).
read_snapshot({Mod, Log0}) ->
    Mod:read_snapshot(Log0).

-spec read_meta(Key :: ra_meta_key(), State :: ra_log()) ->
    maybe(term()).
read_meta(Key, {Mod, Log}) ->
    Mod:read_meta(Key, Log).

-spec read_meta(Key :: ra_meta_key(), State :: ra_log(),
                Default :: term()) -> term().
read_meta(Key, {Mod, Log}, Default) ->
    ra_lib:default(Mod:read_meta(Key, Log), Default).

-spec write_meta(Key :: ra_meta_key(), Value :: term(),
                 State :: ra_log(), Sync :: boolean()) ->
    {ok, ra_log()} | {error, term()}.
write_meta(Key, Value, {Mod, Log}, true) ->
    case Mod:write_meta(Key, Value, Log) of
        {ok, Inner} ->
            ok = Mod:sync_meta(Inner),
            {ok, {Mod, Inner}};
        {error, _} = Err ->
            Err
    end;
write_meta(Key, Value, {Mod, Log}, false) ->
    case Mod:write_meta(Key, Value, Log) of
        {ok, Inner} ->
            {ok, {Mod, Inner}};
        {error, _} = Err ->
            Err
    end.

-spec write_meta(Key :: ra_meta_key(), Value :: term(),
                 State :: ra_log()) ->
    {ok, ra_log()} | {error, term()}.
write_meta(Key, Value, Log) ->
    write_meta(Key, Value, Log, true).


-spec sync_meta(State :: ra_log()) -> ok.
sync_meta({Mod, Log}) ->
    Mod:sync_meta(Log).

-spec exists(ra_idxterm(), ra_log()) -> boolean().
exists({Idx, Term}, Log) ->
    case ra_log:fetch_term(Idx, Log) of
        Term -> true;
        _ -> false
    end.
