%% @hidden
-module(ra_log_snapshot).

-behaviour(ra_snapshot).

-export([
         prepare/2,
         write/3,
         begin_accept/3,
         accept_chunk/2,
         complete_accept/2,
         read/2,
         recover/1,
         validate/1,
         read_meta/1
         ]).

-include("ra.hrl").

-define(MAGIC, "RASN").
-define(VERSION, 1).

-type file_err() :: ra_snapshot:file_err().
-type meta() :: ra_snapshot:meta().

%% DO nothing. There is no preparation for snapshotting
prepare(_Index, State) -> State.

%% @doc
%% Snapshot file format:
%% "RASN"
%% Version (byte)
%% Checksum (unsigned 32)
%% Index (unsigned 64)
%% Term (unsigned 64)
%% Num cluster servers (byte)
%% [DataLen (byte), Data (binary)]
%% Snapshot Data (binary)
%% @end

-spec write(file:filename(), meta(), term()) ->
    ok | {error, file_err()}.
write(Dir, {Idx, Term, ClusterServers}, MacState) ->
    Bin = term_to_binary(MacState),
    Data = [<<Idx:64/unsigned,
              Term:64/unsigned,
              (length(ClusterServers)):8/unsigned>>,
            [begin
                 B = term_to_binary(N),
                 <<(byte_size(B)):16/unsigned,
                   B/binary>>
             end || N <- ClusterServers],
           Bin],
    Checksum = erlang:crc32(Data),
    File = filename(Dir),
    file:write_file(File, [<<?MAGIC,
                             ?VERSION:8/unsigned,
                             Checksum:32/integer>>,
                           Data]).


begin_accept(SnapDir, Crc, {Idx, Term, Cluster}) ->
    File = filename(SnapDir),
    {ok, Fd} = file:open(File, [write, binary, raw]),
    Data = [<<Idx:64/unsigned,
              Term:64/unsigned,
              (length(Cluster)):8/unsigned>>,
            [begin
                 B = term_to_binary(N),
                 <<(byte_size(B)):16/unsigned,
                   B/binary>>
             end || N <- Cluster]],
    PartialCrc = erlang:crc32(Data),
    ok = file:write(Fd, [<<?MAGIC,
                           ?VERSION:8/unsigned,
                           Crc:32/integer>>,
                         Data]),
    {ok, {PartialCrc, Crc, Fd}}.

accept_chunk(Chunk, {PartialCrc0, Crc, Fd}) ->
    ok = file:write(Fd, Chunk),
    PartialCrc = erlang:crc32(PartialCrc0, Chunk),
    {ok, {PartialCrc, Crc, Fd}}.

complete_accept(Chunk, {PartialCrc0, Crc, Fd}) ->
    ok = file:write(Fd, Chunk),
    Crc = erlang:crc32(PartialCrc0, Chunk),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    ok.

read(Size, Dir) ->
    File = filename(Dir),
    case file:open(File, [read, binary, raw]) of
        {ok, Fd} ->
            case read_meta_internal(Fd) of
                {ok, Meta, Crc} ->
                    %% make chunks
                    {ok, Crc, Meta, Fd, make_chunks(Size, Fd, [])};
                {error, _} = Err ->
                    _ = file:close(Fd),
                    Err
            end;
        Err ->
            Err
    end.

make_chunks(Size, Fd, Acc) ->
    {ok, Cur} = file:position(Fd, cur),
    {ok, Eof} = file:position(Fd, eof),
    case min(Size, Eof - Cur) of
        Size ->
            {ok, _} = file:position(Fd, Cur + Size),
            Thunk = fun(F) ->
                            %% Position file offset
                            {ok, _} = file:position(F, Cur),
                            {ok, Data} = file:read(F, Size),
                            {Data, F}
                    end,
            %% ensure pos is correct for next chunk
            {ok, _} = file:position(Fd, Cur + Size),
            make_chunks(Size, Fd, [Thunk | Acc]);
        Rem ->
            %% this is the last thunk
            Thunk = fun(F) ->
                            %% Position file offset
                            {ok, _} = file:position(F, Cur),
                            {ok, Data} = file:read(F, Rem),
                            _ = file:close(F),
                            {Data, F}
                    end,
            lists:reverse([Thunk | Acc])
    end.

-spec recover(file:filename()) ->
    {ok, meta(), term()} |
    {error, invalid_format |
    {invalid_version, integer()} |
    checksum_error |
    file_err()}.
recover(Dir) ->
    File = filename(Dir),
    case file:read_file(File) of
        {ok, <<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer, Data/binary>>} ->
            validate(Crc, Data);
        {ok, <<?MAGIC, Version:8/unsigned, _:32/integer, _/binary>>} ->
            {error, {invalid_version, Version}};
        {ok, _} ->
            {error, invalid_format};
        {error, _} = Err ->
            Err
    end.

validate(Dir) ->
    case recover(Dir) of
        {ok, _, _} -> ok;
        Err -> Err
    end.

%% @doc reads the index and term from the snapshot file without reading the
%% entire binary body. NB: this does not do checksum validation.
-spec read_meta(file:filename()) ->
    {ok, meta()} | {error, invalid_format |
                          {invalid_version, integer()} |
                          checksum_error |
                          file_err()}.
read_meta(Dir) ->
    File = filename(Dir),
    case file:open(File, [read, binary, raw]) of
        {ok, Fd} ->
            case read_meta_internal(Fd) of
                {ok, Meta, _} ->
                    {ok, Meta};
                {error, _} = Err ->
                    _ = file:close(Fd),
                    Err
            end
    end.

%% Internal

read_meta_internal(Fd) ->
    HeaderSize = 9 + 16 + 1,
    case file:read(Fd, HeaderSize) of
        {ok, <<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer,
               Idx:64/unsigned,
               Term:64/unsigned,
               NumServers:8/unsigned>>} ->
            case read_servers(NumServers, [], Fd, HeaderSize) of
                {ok, Servers} ->
                    {ok, {Idx, Term, Servers}, Crc};
                Err ->
                    Err
            end;
        {ok, <<?MAGIC, Version:8/unsigned, _:32/integer, _/binary>>} ->
            {error, {invalid_version, Version}};
        {ok, _} ->
            {error, invalid_format};
        eof ->
            {error, unexpected_eof_when_parsing_header};
        Err ->
            Err
    end.

validate(Crc, Data) ->
    case erlang:crc32(Data) of
        Crc ->
            parse_snapshot(Data);
        _ ->
            {error, checksum_error}
    end.

parse_snapshot(<<Idx:64/unsigned, Term:64/unsigned,
                 NumServers:8/unsigned, Rest0/binary>>) ->
    {Servers, Rest} = parse_servers(NumServers, [], Rest0),
    {ok, {Idx, Term, Servers}, binary_to_term(Rest)}.

read_servers(0, Servers, Fd, Offset) ->
    %% leave the position in the right place
    {ok, _} = file:position(Fd, Offset),
    {ok, lists:reverse(Servers)};
read_servers(Num, Servers, Fd, Offset) ->
    case file:pread(Fd, Offset, 2) of
        {ok, <<Len:16/unsigned>>} ->
            case file:pread(Fd, Offset+2, Len) of
                {ok, <<ServerData:Len/binary>>} ->
                    read_servers(Num - 1,
                                 [binary_to_term(ServerData) | Servers],
                                 Fd,
                                 Offset + 2 + Len);
                eof ->
                    {error, unexpected_eof_when_parsing_servers};
                Err ->
                    Err
            end;
        eof ->
            {error, unexpected_eof_when_parsing_servers};
        Err ->
            Err
    end.

parse_servers(0, Servers, Data) ->
    {lists:reverse(Servers), Data};
parse_servers(Num, Servers,
              <<Len:16/unsigned, ServerData:Len/binary, Rem/binary>>) ->
    parse_servers(Num - 1, [binary_to_term(ServerData) | Servers], Rem).

filename(Dir) ->
    filename:join(Dir, "snapshot.dat").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
