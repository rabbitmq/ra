-module(ra_snapshot).

-include("ra.hrl").

-type file_err() :: file:posix() | badarg | terminated | system_limit.
-type meta() :: {ra_index(), ra_term(), ra_cluster_servers()}.

-export([
    prepare/3,
    write/4,
    save/4,
    read/2,
    recover/2,
    read_indexterm/2
]).
-export_type([meta/0, file_err/0]).

%% Side effect function
%% Turn the current state into immutable reference.
-callback prepare(Index :: ra_index(), State :: term()) -> Ref :: term().

%% Saves snapshot from external state to disk.
%% Runs in a separate process.
%% External storage should be available to read
-callback write(Location :: file:filename(),
                Meta :: meta(),
                Ref :: term()) ->
    ok | {error, file_err() | term()}.

%% Read the snapshot from disk into serialised structure for transfer.
-callback read(Location :: file:filename()) ->
    {ok, Meta :: meta(), Data :: term()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.

%% Dump the snapshot data to disk withtou touching the external state
-callback save(Location :: file:filename(), Meta :: meta(), Data :: term()) ->
    ok | {error, file_err() | term()}.

%% Side-effect function
%% Recover machine state from file
-callback recover(Location :: file:filename()) ->
    {ok, Meta :: meta(), State :: term()} | {error, term()}.

%% Only read index and term from snapshot
-callback read_indexterm(Location :: file:filename()) ->
    {ok, ra_idxterm()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.

-spec prepare(Module :: module(), Index :: ra_index(), State :: term()) ->
    Ref :: term().
prepare(Module, Index, State) ->
    Module:prepare(Index, State).


-spec write(Module :: module(),
            Location :: file:filename(),
            Meta :: meta(),
            Ref :: term()) ->
    ok | {error, file_err() | term()}.
write(Module, Location, Meta, Ref) ->
    Module:write(Location, Meta, Ref).

-spec save(Module :: module(),
               Location :: file:filename(),
               Meta :: meta(),
               Data :: term()) ->
    ok | {error, file_err() | term()}.
save(Module, Location, Meta, Data) ->
    Module:save(Location, Meta, Data).

-spec read(Module :: module(), Location :: file:filename()) ->
    {ok, Meta :: meta(), Data :: term()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.
read(Module, Location) ->
    Module:read(Location).

-spec recover(Module :: module(), Location :: file:filename()) ->
    {ok, Meta :: meta(), State :: term()} | {error, term()}.
recover(Module, Location) ->
    Module:recover(Location).

-spec read_indexterm(Module :: module(), Location :: file:filename()) ->
    {ok, ra_idxterm()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.
read_indexterm(Module, Location) ->
    Module:read_indexterm(Location).



