%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_snapshot).

-behaviour(ra_snapshot).

-include("ra.hrl").
-include_lib("kernel/include/file.hrl").

-export([
         prepare/2,
         write/4,
         write/5,
         list/1,
         delete/1,
         indexes/1,
         encode/3,
         decode_image/1,
         meta_from_image/1,
         sync/1,
         begin_accept/2,
         accept_chunk/2,
         complete_accept/2,
         begin_read/2,
         read_chunk/3,
         recover/1,
         validate/1,
         read_meta/1,
         context/0,
         get_size/1
         ]).

-define(MAGIC, "RASN").
-define(VERSION, 1).
-define(STORE_EPOCH, <<"1">>).
-define(ALIGN, 4096).

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
%% Zero padding up to a multiple of 4096 bytes (covered by the checksum)
%% @end

-spec write(file:filename(), meta(), term(), Sync :: boolean()) ->
    {ok, non_neg_integer()} | {error, file_err()}.
write(Dir, Meta, MacState, Sync) ->
    {Image, Bytes} = encode(Meta, MacState, true),
    File = filename(Dir),
    case ra_lib:write_file(File, Image, Sync) of
        ok ->
            {ok, Bytes};
        Err ->
            Err
    end.

%% @doc The ra_snapshot write/5 callback. Small snapshots are appended to the
%% shared snapshot log of the system (see ra_log_snap_store) if one is running,
%% which makes them durable without creating any files of their own. Anything
%% else, including when the log fails, is written to the `Location' directory
%% as usual.
-spec write(file:filename(), meta(), term(), ra_seq:state(),
            Sync :: boolean()) ->
    {ok, non_neg_integer(), durable | directory} | {error, file_err()}.
write(Location, #{index := Idx, term := Term} = Meta, MacState, Indexes,
      Sync) ->
    case store_location(Location) of
        {ok, #{name := Name, uid := UId, max_size := MaxSize}, _} ->
            {Data, Size0} = encode_data(Meta, MacState),
            IndexesBin = term_to_binary(Indexes),
            case Size0 + byte_size(IndexesBin) =< MaxSize of
                true ->
                    {Image, Size} = finish_image(Data, Size0, false),
                    case store_put(Name, UId, {Idx, Term},
                                   iolist_to_binary(Image), IndexesBin) of
                        ok ->
                            {ok, Size, durable};
                        {error, Reason} ->
                            ?WARN("ra_log_snapshot: ~ts: could not store "
                                  "snapshot ~b in the snapshot log: ~w, "
                                  "writing a directory instead",
                                  [UId, Idx, Reason]),
                            write_directory(Location, Data, Size0, Sync)
                    end;
                false ->
                    write_directory(Location, Data, Size0, Sync)
            end;
        undefined ->
            {Data, Size0} = encode_data(Meta, MacState),
            write_directory(Location, Data, Size0, Sync)
    end.

store_put(Name, UId, IdxTerm, Image, IndexesBin) ->
    try
        ra_log_snap_store:put_bin(Name, UId, ?STORE_EPOCH, IdxTerm, Image,
                                  IndexesBin)
    catch
        exit:Reason ->
            {error, Reason}
    end.

write_directory(Location, Data, Size0, Sync) ->
    ok = ra_lib:make_dir(Location),
    {Image, Bytes} = finish_image(Data, Size0, true),
    case ra_lib:write_file(filename(Location), Image, Sync) of
        ok -> {ok, Bytes, directory};
        Err -> Err
    end.

%% @doc The ra_snapshot list/1 callback: the snapshot directories plus the
%% member's snapshot in the snapshot log, if it has one.
-spec list(file:filename()) -> [file:filename()].
list(SnapshotsDir) ->
    {ok, Names} = prim_file:list_dir(SnapshotsDir),
    case store_for_dir(SnapshotsDir) of
        {ok, #{name := Name, uid := UId}} ->
            case store_reconcile(Name, UId) of
                {ok, #{idx := Idx, term := Term}} ->
                    Virtual = binary_to_list(
                                ra_snapshot:snapshot_name(Idx, Term)),
                    case lists:member(Virtual, Names) of
                        true -> Names;
                        false -> [Virtual | Names]
                    end;
                not_found ->
                    Names
            end;
        undefined ->
            Names
    end.

store_reconcile(Name, UId) ->
    try
        ra_log_snap_store:reconcile(Name, UId, ?STORE_EPOCH)
    catch
        exit:Reason ->
            ?WARN("ra_log_snapshot: ~ts: snapshot log unavailable: ~w",
                  [UId, Reason]),
            not_found
    end.

%% @doc The ra_snapshot delete/1 callback.
-spec delete(file:filename()) -> ok.
delete(Location) ->
    case store_location(Location) of
        {ok, #{name := Name, uid := UId}, {Idx, _Term}} ->
            %% the snapshot log entry is dead if it is not newer than this
            ra_log_snap_store:release(Name, UId, Idx);
        undefined ->
            ok
    end,
    ra_lib:recursive_delete(Location).

%% @doc The ra_snapshot indexes/1 callback.
-spec indexes(file:filename()) ->
    {ok, ra_seq:state()} | {error, term()}.
indexes(Location) ->
    case ra_lib:is_dir(Location) of
        true ->
            ra_snapshot:indexes(Location);
        false ->
            case store_read(Location) of
                {ok, _Image, Indexes} -> {ok, Indexes};
                {error, enoent} -> {ok, []};
                {error, _} = Err -> Err
            end
    end.

%% @doc encodes the complete snapshot file image (header, checksum, meta and
%% machine state) without writing it anywhere. Returns the image and its size
%% in bytes. When `Pad' is true the image is zero padded to a multiple of
%% 4096 bytes so that many parallel writers do not leave partial tail pages
%% that the file system / device has to merge. binary_to_term/1 ignores the
%% trailing bytes and the checksum covers them so no format change is needed.
-spec encode(meta(), term(), Pad :: boolean()) ->
    {iodata(), non_neg_integer()}.
encode(Meta, MacState, Pad) ->
    {Data, Size} = encode_data(Meta, MacState),
    finish_image(Data, Size, Pad).

%% serialises the body of the image, the expensive part
encode_data(Meta, MacState) ->
    %% no compression on meta data to make sure reading it is as fast
    %% as possible
    MetaBin = term_to_binary(Meta),
    IOVec = term_to_iovec(MacState),
    Data = [<<(byte_size(MetaBin)):32/unsigned>>, MetaBin | IOVec],
    {Data, 9 + iolist_size(Data)}.

finish_image(Data0, Bytes0, Pad) ->
    PadBytes = case Pad of
                   true -> (?ALIGN - (Bytes0 rem ?ALIGN)) rem ?ALIGN;
                   false -> 0
               end,
    Data = [Data0, <<0:(PadBytes * 8)>>],
    Checksum = erlang:crc32(Data),
    {[<<?MAGIC, ?VERSION:8/unsigned, Checksum:32/integer>>, Data],
     Bytes0 + PadBytes}.

%% @doc validates and decodes a snapshot file image held in memory. The
%% counterpart of recover/1 for images that did not come from a file.
-spec decode_image(binary()) ->
    {ok, meta(), term()} |
    {error, invalid_format |
     {invalid_version, integer()} |
     checksum_error}.
decode_image(<<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer, Data/binary>>) ->
    validate(Crc, Data);
decode_image(<<?MAGIC, Version:8/unsigned, _:32/integer, _/binary>>) ->
    {error, {invalid_version, Version}};
decode_image(_) ->
    {error, invalid_format}.

%% @doc reads the meta data from a snapshot file image held in memory. NB: as
%% with read_meta/1 this does not do checksum validation.
-spec meta_from_image(binary()) ->
    {ok, meta()} |
    {error, invalid_format | {invalid_version, integer()}}.
meta_from_image(<<?MAGIC, ?VERSION:8/unsigned, _Crc:32/integer,
                  MetaSize:32/unsigned, MetaBin:MetaSize/binary,
                  _/binary>>) ->
    {ok, binary_to_term(MetaBin)};
meta_from_image(<<?MAGIC, Version:8/unsigned, _:32/integer, _/binary>>)
  when Version =/= ?VERSION ->
    {error, {invalid_version, Version}};
meta_from_image(_) ->
    {error, invalid_format}.

-spec sync(file:filename()) ->
    ok | {error, file_err()}.
sync(Dir) ->
    File = filename(Dir),
    ra_lib:sync_file(File).

begin_accept(SnapDir, Meta) ->
    File = filename(SnapDir),
    {ok, Fd} = file:open(File, [write, binary, raw]),
    MetaBin = term_to_binary(Meta),
    Data = [<<(byte_size(MetaBin)):32/unsigned>>, MetaBin],
    PartialCrc = erlang:crc32(Data),
    Chunk = [<<?MAGIC,
               ?VERSION:8/unsigned,
               0:32/integer>>,
             Data],
    Bytes = iolist_size(Chunk),
    ok = file:write(Fd, Chunk),
    {ok, {Bytes, PartialCrc, Fd}}.

accept_chunk(<<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer,
               Rest/binary>> = Chunk, {_Bytes, _PartialCrc, Fd}) ->
    % ensure we overwrite the existing header when we are receiving the
    % full file
    PartialCrc = erlang:crc32(Rest),
    Bytes = iolist_size(Chunk),
    {ok, 0} = file:position(Fd, 0),
    ok = file:write(Fd, Chunk),
    {ok, {Bytes, PartialCrc, Crc, Fd}};
accept_chunk(Chunk, {Bytes, PartialCrc, Fd}) ->
    %% compatibility clause where we did not receive the full file
    %% do not validate Crc due to OTP 26 map key ordering changes
    <<_Crc:32/integer, Rest/binary>> = Chunk,
    accept_chunk(Rest, {Bytes, PartialCrc, undefined, Fd});
accept_chunk(Chunk, {Bytes, PartialCrc0, Crc, Fd}) ->
    Bytes1 = Bytes + iolist_size(Chunk),
    ok = file:write(Fd, Chunk),
    PartialCrc = erlang:crc32(PartialCrc0, Chunk),
    {ok, {Bytes1, PartialCrc, Crc, Fd}}.

complete_accept(Chunk, St0) ->
    {ok, {Bytes, CalculatedCrc, Crc, Fd}} = accept_chunk(Chunk, St0),
    CrcToWrite = case Crc of
                     undefined ->
                         CalculatedCrc;
                     _ ->
                         Crc
                 end,
    ok = file:pwrite(Fd, 5, <<CrcToWrite:32/integer>>),
    ok = ra_file:sync(Fd),
    ok = file:close(Fd),
    % elp:ignore W0060 (bound_var_in_lhs)
    CalculatedCrc = CrcToWrite,
    {ok, Bytes}.

begin_read(Dir, Context) ->
    File = filename(Dir),
    case file:open(File, [read, binary, raw]) of
        {error, enoent} = Err ->
            case store_read(Dir) of
                {ok, Image, _} ->
                    begin_read_image(Image, Context);
                {error, _} ->
                    Err
            end;
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

read_chunk({mem, Image, Pos}, Size, _Dir) ->
    Eof = byte_size(Image),
    Data = binary:part(Image, Pos, min(Size, Eof - Pos)),
    case Pos + Size >= Eof of
        true ->
            {ok, Data, last};
        false ->
            {ok, Data, {next, {mem, Image, Pos + Size}}}
    end;
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
        {ok, Image} ->
            decode_image(Image);
        {error, enoent} = Err ->
            case store_read(Dir) of
                {ok, Image, _} ->
                    decode_image(Image);
                {error, _} ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.


validate(Dir) ->
    case prim_file:read_file(filename(Dir)) of
        {ok, Image} ->
            case decode_image(Image) of
                {ok, _, _} -> ok;
                Err -> Err
            end;
        {error, enoent} = Err ->
            %% the snapshot log checks the checksum of every record it reads
            case store_read(Dir) of
                {ok, _, _} -> ok;
                {error, _} -> Err
            end;
        {error, _} = Err ->
            Err
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
            end;
        {error, enoent} = Err ->
            case store_read(Dir) of
                {ok, Image, _} ->
                    meta_from_image(Image);
                {error, _} ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec get_size(file:filename()) ->
    {ok, non_neg_integer()} | {error, file_err()}.
get_size(Dir) ->
    File = filename(Dir),
    case prim_file:read_file_info(File) of
        {ok, #file_info{size = Size}} ->
            {ok, Size};
        {error, enoent} = Err ->
            case store_location(Dir) of
                {ok, #{name := Name, uid := UId}, {Idx, Term}} ->
                    case ra_log_snap_store:lookup(Name, UId) of
                        {ok, #{idx := Idx, term := Term, size := Size}} ->
                            {ok, Size};
                        _ ->
                            Err
                    end;
                undefined ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec context() -> map().
context() ->
    #{can_accept_full_file => true}.

%% Internal

begin_read_image(<<?MAGIC, ?VERSION:8/unsigned, Crc:32/integer,
                   MetaSize:32/unsigned, MetaBin:MetaSize/binary,
                   _/binary>> = Image, Context) ->
    Meta = binary_to_term(MetaBin),
    case maps:get(can_accept_full_file, Context, false) of
        true ->
            {ok, Meta, {mem, Image, 0}};
        false ->
            {ok, Meta, {Crc, {mem, Image, 9 + 4 + MetaSize}}}
    end;
begin_read_image(_, _) ->
    {error, invalid_format}.

%% Snapshot locations are <data_dir>/<uid>/snapshots/<term>_<index>. If a
%% snapshot log is running for the system that owns <data_dir> the snapshot of
%% the location may be held in it rather than in a directory.
store_location(Location) ->
    case ra_snapshot:parse_snapshot_name(filename:basename(Location)) of
        {ok, IdxTerm} ->
            case store_for_dir(filename:dirname(Location)) of
                {ok, Store} ->
                    {ok, Store, IdxTerm};
                undefined ->
                    undefined
            end;
        error ->
            undefined
    end.

store_for_dir(SnapshotsDir) ->
    case unicode:characters_to_binary(filename:basename(SnapshotsDir)) of
        <<"snapshots">> ->
            ServerDir = filename:dirname(SnapshotsDir),
            UId = unicode:characters_to_binary(filename:basename(ServerDir)),
            Key = ra_log_snap_store:registry_key(filename:dirname(ServerDir)),
            case persistent_term:get(Key, undefined) of
                undefined ->
                    undefined;
                Store ->
                    {ok, Store#{uid => UId}}
            end;
        _ ->
            undefined
    end.

%% reads a snapshot held in the snapshot log, errors are as if the file did
%% not exist unless the log itself failed
store_read(Location) ->
    case store_location(Location) of
        {ok, #{name := Name, uid := UId}, IdxTerm} ->
            try ra_log_snap_store:read(Name, UId, IdxTerm) of
                {ok, _, _} = Ok ->
                    Ok;
                {error, Reason}
                  when Reason == not_found orelse Reason == superseded ->
                    {error, enoent};
                {error, _} = Err ->
                    Err
            catch
                exit:Reason ->
                    {error, Reason}
            end;
        undefined ->
            {error, enoent}
    end.

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
