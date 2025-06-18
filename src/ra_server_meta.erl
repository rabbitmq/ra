%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
-module(ra_server_meta).

-include_lib("stdlib/include/assert.hrl").

-export([
         path/2,
         fetch/3,
         fetch_from_file/1,
         store_sync/4,
         update_last_applied/2
        ]).

%% This module implements persistance for server metadata
%% Before Ra 3.0, metadata was stored in a DETS file, shared
%% by all Ra servers in a Ra system.
%% Now, we store each server's metadata in a separate file,
%% in the server's data directory.
%% The structure of the metadata file is as follows:
%% - 4 bytes magic header (RAM1)
%% - 1004 bytes VotedFor field, which is a binary
%%   - 2 bytes for the size of the first atom (server name)
%%   - first atom (server name) as a binary
%%   - 2 bytes for the size of the second atom (node name)
%%   - second atom (node name) as a binary
%%   - padding (zeroed)
%% - 8 bytes CurrentTerm (unsigned 64-bit integer)
%% - 8 bytes LastApplied (unsigned 64-bit integer)
%% for a total of 1024 bytes
%%
%% When VotedFor/Term change, the file is updated and fsynced.
%% If only the LastApplied changes, we update but do not fsync,
%% since this would be prohibitively slow.

-define(FILENAME, "server.meta").
-define(MAGIC, "RAM1").
-define(TOTAL_SIZE, 1024).
-define(VOTED_FOR_MAX_SIZE, 1004). %% TOTAL_SIZE minus everything else
-define(LAST_APPLIED_POSITION, ?TOTAL_SIZE - 8).

path(DataDir, UId) ->
    ServerDir = filename:join(DataDir, UId),
    filename:join(ServerDir, ?FILENAME).

fetch(Path, MetaName, UId) ->
    case fetch_from_file(Path) of
        {ok, Metadata} when is_tuple(Metadata) ->
            {ok, Metadata};
        {error, _} ->
            %% metadata migration case:
            %% fetch from ra_log_meta and store in a file
            {VotedFor, CurrentTerm, LastApplied} = fetch_from_ra_log_meta(MetaName, UId),
            case store_sync(Path, VotedFor, CurrentTerm, LastApplied) of
                ok ->
                    ra_log_meta:delete(MetaName, UId),
                    {ok, {VotedFor, CurrentTerm, LastApplied}};
                Err ->
                    Err
            end
    end.

fetch_from_file(Path) ->
    case file:read_file(Path) of
        {ok, <<?MAGIC, VotedForBin:1004/binary, CurrentTerm:64/unsigned, LastApplied:64/unsigned>>} ->
            VotedFor = try
                           parse_voted_for(VotedForBin)
                       catch
                           _:_ -> undefined
                       end,
            {ok, {VotedFor, CurrentTerm, LastApplied}};
        {ok, _} ->
            {error, invalid_format};
        Err ->
            Err
    end.

fetch_from_ra_log_meta(MetaName, UId) ->
    VotedFor = ra_log_meta:fetch(MetaName, UId, voted_for, undefined),
    CurrentTerm = ra_log_meta:fetch(MetaName, UId, current_term, 0),
    LastApplied = ra_log_meta:fetch(MetaName, UId, last_applied, 0),
    {VotedFor, CurrentTerm, LastApplied}.

store_sync(MetaFile, VotedFor, CurrentTerm, LastApplied) when is_binary(MetaFile) ->
    {ok, MetaFd} = file:open(MetaFile, [write, binary, raw]),
    store_sync(MetaFd, VotedFor, CurrentTerm, LastApplied),
    file:close(MetaFd);
store_sync(MetaFd, VotedFor, CurrentTerm, LastApplied) ->
    Data = encode_metadata(VotedFor, CurrentTerm, LastApplied),
    ok = file:pwrite(MetaFd, 0, Data),
    ok = file:sync(MetaFd).

update_last_applied(MetaFd, LastApplied) ->
    ok = file:pwrite(MetaFd, ?LAST_APPLIED_POSITION, <<LastApplied:64>>).

encode_metadata(VotedFor, CurrentTerm, LastApplied) ->
    VotedForBin = case VotedFor of
                      undefined ->
                          <<>>;
                      {NameAtom, NodeAtom} ->
                          NameAtomBin = atom_to_binary(NameAtom, utf8),
                          NodeAtomBin = atom_to_binary(NodeAtom, utf8),
                          NameSize = byte_size(NameAtomBin),
                          NodeSize = byte_size(NodeAtomBin),
                          <<NameSize:16/unsigned, NameAtomBin/binary,
                            NodeSize:16/unsigned, NodeAtomBin/binary>>
                  end,

    VotedForSize = byte_size(VotedForBin),
    PaddingSize = ?VOTED_FOR_MAX_SIZE - VotedForSize,
    case PaddingSize >= 0 of
        true ->
            Padding = <<0:PaddingSize/unit:8>>,
            <<?MAGIC, VotedForBin/binary, Padding/binary,
            CurrentTerm:64/unsigned, LastApplied:64/unsigned>>;
        false ->
            vote_for_binary_too_long
        end.

parse_voted_for(<<NameAtomSize:16/unsigned, Rest/binary>>) when NameAtomSize > 0 ->
    case Rest of
        <<NameAtom:NameAtomSize/binary, NodeAtomSize:16/unsigned, NodeAtom:NodeAtomSize/binary, _/binary>>
          when NodeAtomSize > 0 ->
            {binary_to_atom(NameAtom, utf8), binary_to_atom(NodeAtom, utf8)};
        _ ->
            undefined
    end;
parse_voted_for(_) ->
    undefined.

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

v1_format_test() ->
    CurrentTerm = rand:uniform(10000),
    LastApplied = rand:uniform(100000),
    VotedFor = {somename, somenode},

    % we always encode into a 1024-byte binary
    Data = encode_metadata(VotedFor, CurrentTerm, LastApplied),
    ?assertEqual(1024, byte_size(Data)),

    % we can reconstruct the VotedFor from the binary
    <<"RAM1", VotedForBin/binary>> = Data,
    ?assertEqual({somename, somenode}, parse_voted_for(VotedForBin)),

    % we can extract term and last applied from fixed positions
    <<_:1008/binary, ParsedTerm:64/unsigned, ParsedLastApplied:64/unsigned>> = Data,
    ?assertEqual(CurrentTerm, ParsedTerm),
    ?assertEqual(LastApplied, ParsedLastApplied),

    % "empty" metadata
    EmptyData = encode_metadata(undefined, 0, 0),
    ?assertEqual(1024, byte_size(EmptyData)),
    <<"RAM1", VotedForDataUndef/binary>> = EmptyData,
    ?assertEqual(undefined, parse_voted_for(VotedForDataUndef)),
    <<_:1008/binary, ZeroTerm:64/unsigned, ZeroLastApplied:64/unsigned>> = EmptyData,
    ?assertEqual(ZeroTerm, 0),
    ?assertEqual(ZeroLastApplied, 0),

    % end-to-end test
    TempFile = "test_new_meta", %% TODO - put in the right place
    file:write_file(TempFile, Data),
    {ok, {E2EVotedFor, E2ECurrentTerm, E2ELastApplied}} = fetch_from_file(TempFile),
    file:delete(TempFile),
    ?assertEqual(VotedFor, E2EVotedFor),
    ?assertEqual(CurrentTerm, E2ECurrentTerm),
    ?assertEqual(LastApplied, E2ELastApplied),

    % Test edge cases

    % very long atom names, including UTF-8 in on of the atoms
    LongName = list_to_atom([$Σ || _ <- lists:seq(1, 255)]),
    LongNode = list_to_atom([$a || _ <- lists:seq(1, 255)]),
    LongVotedFor = {LongName, LongNode},
    DataLong = encode_metadata(LongVotedFor, 999999, 888888),
    ?assertEqual(1024, byte_size(DataLong)),
    <<"RAM1", VotedForDataLong/binary>> = DataLong,
    ?assertEqual(LongVotedFor, parse_voted_for(VotedForDataLong)),

    % single character atoms
    ShortVotedFor = {a, b},
    DataShort = encode_metadata(ShortVotedFor, 1, 2),
    ?assertEqual(1024, byte_size(DataShort)),
    <<"RAM1", VotedForDataShort/binary>> = DataShort,
    ?assertEqual(ShortVotedFor, parse_voted_for(VotedForDataShort)),

    % max values are handled
    MaxTerm = 18446744073709551615, % 2^64 - 1
    MaxApplied = 18446744073709551615,
    DataMax = encode_metadata(VotedFor, MaxTerm, MaxApplied),
    ?assertEqual(1024, byte_size(DataMax)),
    <<_:1008/binary, ParsedMaxTerm:64/unsigned, ParsedMaxApplied:64/unsigned>> = DataMax,
    ?assertEqual(MaxTerm, ParsedMaxTerm),
    ?assertEqual(MaxApplied, ParsedMaxApplied),

    % invalid magic header
    BadHeaderData = <<"ACME", VotedForBin/binary>>,
    TempFileBadHeader = "test_bad_header", %% TODO path
    file:write_file(TempFileBadHeader, BadHeaderData),
    ?assertEqual({error, invalid_format}, fetch_from_file(TempFileBadHeader)),
    file:delete(TempFileBadHeader),

    ok.

-endif.
