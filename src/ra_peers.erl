-module(ra_peers).

-export([
         init/2,
         all/1,
         peer_ids/1,
         is_member/2,
         add/2,
         remove/2,
         peer_match_indexes/1,
         match_and_next_index/2,
         query_index/2,
         set_next_idx/3,
         set_match_idx/3,
         set_commit_index/3,
         set_query_index/3,
         set_match_and_next_index/4,
         update_post_snapshot_installation/3,
         make_pipeline_rpc_effect/3,
         not_sending_snapshots/1,
         stale/2
         ]).

-include("ra.hrl").

%% holds static or rarely changing fields
-record(peer, {type = full :: full | non_voting,
               status = normal :: normal | {sending_snapshot, pid()},
               next_index = 1 :: ra_index(),
               match_index = 0 :: ra_index(),
               commit_index_sent = 0 :: ra_index(),
               query_index = 0 :: ra_index()}).

-record(?MODULE, {self :: ra_server_id(),
                  peers = #{} :: #{ra_server_id() => #peer{}}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
             ]).

-define(MAX_PIPELINE_DISTANCE, 10000).

-define(UPDATE_PEER(PeerId, Field, Value, State),
    Peers0 = State#?MODULE.peers,
    Peer = maps:get(PeerId, Peers0),
    Peers = maps:put(PeerId, Peer#peer{Field = Value}, Peers0),
    State#?MODULE{peers = Peers}).

init(Self, Members0) ->
    Members = lists:delete(Self, Members0),
    Peers = lists:foldl(fun(N, Acc) ->
                                Acc#{N => #peer{}}
                        end, #{}, Members),
    #?MODULE{self = Self,
             peers = Peers}.

all(#?MODULE{self = Self,
             peers = Peers}) ->
    [Self | maps:keys(Peers)].

peer_ids(#?MODULE{peers = Peers}) ->
    maps:keys(Peers).

is_member(PeerId, #?MODULE{self = PeerId}) ->
    true;
is_member(PeerId, #?MODULE{peers = Peers}) ->
    maps:is_key(PeerId, Peers).

add(PeerId, #?MODULE{peers = Peers} = State) ->
    case is_member(PeerId, State) of
        true ->
            State;
        false ->
            State#?MODULE{peers = maps:put(PeerId, #peer{}, Peers)}
    end.

remove(PeerId, #?MODULE{peers = Peers} = State) ->
    State#?MODULE{peers = maps:remove(PeerId, Peers)}.

peer_match_indexes(#?MODULE{peers = Peers}) ->
    [M || #peer{match_index = M} <- maps:values(Peers)].

match_and_next_index(PeerId, #?MODULE{peers = Peers}) ->
    #peer{match_index = MI, next_index = NI} = maps:get(PeerId, Peers),
    {MI, NI}.

query_index(PeerId, #?MODULE{peers = Peers}) ->
    #peer{query_index = QI} = maps:get(PeerId, Peers),
    QI.

set_next_idx(PeerId, NextIdx, State) ->
    ?UPDATE_PEER(PeerId, next_index, NextIdx, State).

set_match_idx(PeerId, NextIdx, State) ->
    ?UPDATE_PEER(PeerId, match_index, NextIdx, State).

set_commit_index(PeerId, CommitIndex, State) ->
    ?UPDATE_PEER(PeerId, commit_index_sent, CommitIndex, State).

set_query_index(PeerId, QueryIndex, State) ->
    ?UPDATE_PEER(PeerId, query_index, QueryIndex, State).

set_match_and_next_index(PeerId, MatchIdx, NextIdx, State) ->
    Peers0 = State#?MODULE.peers,
    Peer = maps:get(PeerId, Peers0),
    Peers = maps:put(PeerId, Peer#peer{match_index = MatchIdx,
                                       next_index = NextIdx}, Peers0),
    State#?MODULE{peers = Peers}.

update_post_snapshot_installation(PeerId, Idx, State) ->
    Peers0 = State#?MODULE.peers,
    Peer = maps:get(PeerId, Peers0),
    Peers = maps:put(PeerId, Peer#peer{status = normal,
                                       match_index = Idx,
                                       commit_index_sent = Idx,
                                       next_index = Idx + 1}, Peers0),
    State#?MODULE{peers = Peers}.

make_pipeline_rpc_effect(CommitIndex, NextLogIdx,
                         #?MODULE{peers = Peers}) ->
    PeerIds = maps:fold(
                fun (_, #peer{status = {sending_snapshot, _}}, Acc) ->
                        %% if a peers is currently receiving a snapshot
                        %% we should not pipeline
                        Acc;
                    (PeerId, #peer{next_index = NI,
                                   commit_index_sent = CI,
                                   match_index = MI}, Acc)
                      when NI < NextLogIdx orelse CI < CommitIndex ->
                        % there are unsent items or a new commit index
                        % check if the match index isn't too far behind the
                        % next index
                        case NI - MI < ?MAX_PIPELINE_DISTANCE of
                            true ->
                                [PeerId | Acc];
                            false ->
                                Acc
                        end;
                    (_, _, Acc) ->
                        Acc
                end, [], Peers),
    case PeerIds of
        [] -> [];
        _ ->
            [{pipeline_rpcs, PeerIds}]
    end.

not_sending_snapshots(#?MODULE{peers = Peers}) ->
    maps:keys(
      maps:filter(fun (_, #peer{status = {sending_snapshot, _}}) ->
                          false;
                      (_,  _) ->
                          % there are unconfirmed items
                          true
                  end, Peers)).

stale(CommitIndex, #?MODULE{peers = Peers}) ->
    maps:keys(
      maps:filter(fun (_, #peer{status = {sending_snapshot, _}}) ->
                          false;
                      (_, #peer{next_index = NI,
                                match_index = MI})
                        when MI < NI - 1 ->
                          % there are unconfirmed items
                          true;
                      (_, #peer{commit_index_sent = CI})
                        when CI < CommitIndex ->
                          % the commit index has been updated
                          true;
                      (_, _Peer) ->
                          false
                  end, Peers)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
