%% @hidden
-module(ra_log_snapshot).

-behaviour(ra_snapshot).

-export([
         prepare/2,
         write/3,
         save/3,
         read/1,
         recover/1,
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
write(File, {Idx, Term, ClusterServers}, MacState) ->
    Bin = term_to_binary(MacState),
    Data = [<<Idx:64/unsigned,
              Term:64/unsigned,
              (length(ClusterServers)):8/unsigned>>,
            [begin
                 B = term_to_binary(N),
                 <<(byte_size(B)):8/unsigned,
                   B/binary>>
             end || N <- ClusterServers],
           Bin],
    Checksum = erlang:crc32(Data),
    file:write_file(File, [<<?MAGIC,
                             ?VERSION:8/unsigned,
                             Checksum:32/integer>>,
                           Data]).

-spec save(file:filename(), meta(), term()) ->
    ok | {error, file_err()}.
save(File, Meta, Data) -> write(File, Meta, Data).


-spec read(file:filename()) ->
    {ok, meta(), term()} | {error, invalid_format |
                     {invalid_version, integer()} |
                     checksum_error |
                     file_err()}.
read(File) ->
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

-spec recover(file:filename()) ->
    {ok, meta(), term()} |
    {error, invalid_format |
    {invalid_version, integer()} |
    checksum_error |
    file_err()}.
recover(File) -> read(File).

%% @doc reads the index and term from the snapshot file without reading the
%% entire binary body. NB: this does not do checksum validation.
-spec read_meta(file:filename()) ->
    {ok, meta()} | {error, invalid_format |
                          {invalid_version, integer()} |
                          checksum_error |
                          file_err()}.
read_meta(File) ->
    case file:open(File, [read, binary, raw]) of
        {ok, Fd} ->
            HeaderSize = 9 + 16 + 1,
            case file:read(Fd, HeaderSize) of
                {ok, <<?MAGIC, ?VERSION:8/unsigned, _:32/integer,
                       Idx:64/unsigned,
                       Term:64/unsigned,
                       NumServers:8/unsigned>>} ->
                    case read_servers(NumServers, [], Fd, HeaderSize) of
                        {ok, Servers} ->
                            {ok, {Idx, Term, Servers}};
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
            end;
        {error, _} = Err ->
            Err
    end.

%% Internal

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

read_servers(0, Servers, _, _) ->
    {ok, lists:reverse(Servers)};
read_servers(Num, Servers, Fd, Offset) ->
    case file:pread(Fd, Offset, 1) of
        {ok, <<Len:8/unsigned>>} ->
            case file:pread(Fd, Offset+1, Len) of
                {ok, <<ServerData:Len/binary>>} ->
                    read_servers(Num - 1,
                                 [binary_to_term(ServerData) | Servers],
                                 Fd,
                                 Offset + 1 + Len);
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
              <<Len:8/unsigned, ServerData:Len/binary, Rem/binary>>) ->
    parse_servers(Num - 1, [binary_to_term(ServerData) | Servers], Rem).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
