%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_snapshot).

-behaviour(ra_snapshot).

-export([
         prepare/2,
         write/4,
         sync/1,
         begin_accept/2,
         accept_chunk/2,
         complete_accept/2,
         begin_read/2,
         read_chunk/3,
         recover/1,
         validate/1,
         read_meta/1,
         context/0
         ]).

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
%% MetaData Len (unsigned 32)
%% MetaData (binary)
%% Snapshot Data (binary)
%% @end

-spec write(file:filename(), meta(), term(), Sync :: boolean()) ->
    {ok, non_neg_integer()} | {error, file_err()}.
write(Dir, Meta, MacState, Sync) ->
    %% no compression on meta data to make sure reading it is as fast
    %% as possible
    MetaBin = term_to_binary(Meta),
    IOVec = term_to_iovec(MacState),
    Data = [<<(size(MetaBin)):32/unsigned>>, MetaBin | IOVec],
    Checksum = erlang:crc32(Data),
    File = filename(Dir),
    Bytes = 9 + iolist_size(Data),
    case ra_lib:write_file(File, [<<?MAGIC,
                                    ?VERSION:8/unsigned,
                                    Checksum:32/integer>>,
                                  Data], Sync) of
        ok ->
            {ok, Bytes};
        Err ->
            Err
    end.

-spec sync(file:filename()) ->
    ok | {error, file_err()}.
sync(Dir) ->
    File = filename(Dir),
    ra_lib:sync_file(File).

begin_accept(SnapDir, Meta) ->
    File = filename(SnapDir),
    {ok, Fd} = file:open(File, [write, binary, raw]),
    MetaBin = term_to_binary(Meta),
    Data = [<<(size(MetaBin)):32/unsigned>>, MetaBin],
    PartialCrc = erlang:crc32(Data),
    ok = file:write(Fd, [<<?MAGIC,
                           ?VERSION:8/unsigned,
                           0:32/integer>>,
                         Data]),
    {ok, {PartialCrc, Fd}}.

accept_chunk(<<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer,
               Rest/binary>> = Chunk, {_PartialCrc, Fd}) ->
    % ensure we overwrite the existing header when we are receiving the
    % full file
    PartialCrc = erlang:crc32(Rest),
    {ok, 0} = file:position(Fd, 0),
    ok = file:write(Fd, Chunk),
    {ok, {PartialCrc, Crc, Fd}};
accept_chunk(Chunk, {PartialCrc, Fd}) ->
    %% compatibility clause where we did not receive the full file
    %% do not validate Crc due to OTP 26 map key ordering changes
    <<_Crc:32/integer, Rest/binary>> = Chunk,
    accept_chunk(Rest, {PartialCrc, undefined, Fd});
accept_chunk(Chunk, {PartialCrc0, Crc, Fd}) ->
    ok = file:write(Fd, Chunk),
    PartialCrc = erlang:crc32(PartialCrc0, Chunk),
    {ok, {PartialCrc, Crc, Fd}}.

complete_accept(Chunk, St0) ->
    {ok, {CalculatedCrc, Crc, Fd}} = accept_chunk(Chunk, St0),
    CrcToWrite = case Crc of
                     undefined ->
                         CalculatedCrc;
                     _ ->
                         Crc
                 end,
    ok = file:pwrite(Fd, 5, <<CrcToWrite:32/integer>>),
    ok = ra_file:sync(Fd),
    ok = file:close(Fd),
    CalculatedCrc = CrcToWrite,
    ok.

begin_read(Dir, Context) ->
    File = filename(Dir),
    case file:open(File, [read, binary, raw]) of
        {ok, Fd} ->
            case read_meta_internal(Fd) of
                {ok, Meta, _Crc}
                  when map_get(can_accept_full_file, Context) ->
                    {ok, Eof} = file:position(Fd, eof),
                    {ok, Meta, {0, Eof, Fd}};
                {ok, Meta, Crc} ->
                    {ok, Cur} = file:position(Fd, cur),
                    {ok, Eof} = file:position(Fd, eof),
                    {ok, Meta, {Crc, {Cur, Eof, Fd}}};
                {error, _} = Err ->
                    _ = file:close(Fd),
                    Err
            end;
        Err ->
            Err
    end.

read_chunk({Crc, ReadState}, Size, Dir) when is_integer(Crc) ->
    %% this the compatibility read mode for old snapshot receivers
    case read_chunk(ReadState, Size - 4, Dir) of
        {ok, Data, ReadState1} ->
            {ok, <<Crc:32/integer, Data/binary>>, ReadState1};
        {error, _} = Err ->
            Err
    end;
read_chunk({Pos, Eof, Fd}, Size, _Dir) ->
    case file:pread(Fd, Pos, Size) of
        {ok, Data} ->
            case Pos + Size >= Eof of
                true ->
                    _ = file:close(Fd),
                    {ok, Data, last};
                false ->
                    {ok, Data, {next, {Pos + Size, Eof, Fd}}}
            end;
        {error, _} = Err ->
            Err;
        eof ->
            {error, unexpected_eof}
    end.

-spec recover(file:filename_all()) ->
    {ok, meta(), term()} |
    {error, invalid_format |
     {invalid_version, integer()} |
     checksum_error |
     file_err()}.
recover(Dir) ->
    File = filename(Dir),
    case prim_file:read_file(File) of
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
                {ok, Meta, _Crc} ->
                    _ = file:close(Fd),
                    {ok, Meta};
                Err ->
                    _ = file:close(Fd),
                    Err
            end
    end.

-spec context() -> map().
context() ->
    #{can_accept_full_file => true}.

%% Internal

read_meta_internal(Fd) ->
    HeaderSize = 9 + 4,
    case file:read(Fd, HeaderSize) of
        {ok, <<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer,
               MetaSize:32/unsigned>>} ->
            case file:read(Fd, MetaSize) of
                {ok, MetaBin} ->
                    {ok, binary_to_term(MetaBin), Crc};
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

parse_snapshot(<<MetaSize:32/unsigned, MetaBin:MetaSize/binary,
                 Rest/binary>>) ->
    Meta = binary_to_term(MetaBin),
    {ok, Meta, binary_to_term(Rest)}.

filename(Dir) ->
    filename:join(Dir, "snapshot.dat").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
