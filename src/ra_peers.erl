-module(ra_peers).

-export([
         init/2,
         all/1,
         count/1,
         peer_ids/1,
         is_member/2,
         add/2,
         remove/2,
         peer_match_indexes/1,
         match_and_next_index/2,
         query_index/2,
         match_index/2,
         commit_index_sent/2,
         next_index/2,
         status/2,
         set_next_index/3,
         set_match_index/3,
         set_commit_index/3,
         set_query_index/3,
         set_status/3,
         set_match_and_next_index/4,
         reset_peer/3,
         update_post_snapshot_installation/3,
         make_pipeline_rpc_effect/3,
         not_sending_snapshots/1,
         stale/3
         ]).

-include("ra.hrl").

-type peer_status() :: normal | {sending_snapshot, pid()}.

%% holds static or rarely changing fields
-record(peer, {type = full :: full | non_voting,
               status = normal :: peer_status(),
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

-define(GET_PEER(PeerId, Field, State),
        #{PeerId := #peer{Field = Value}} = State#?MODULE.peers,
        Value).

-spec init(ra_server_id(), [ra_server_id()]) ->
    state().
init(Self, Members0) ->
    ?INFO("~w ra_peers:init ~w", [Self, Members0]),
    Members = lists:delete(Self, Members0),
    Peers = lists:foldl(fun(N, Acc) ->
                                Acc#{N => #peer{}}
                        end, #{}, Members),
    #?MODULE{self = Self,
             peers = Peers}.

-spec all(state()) -> [ra_server_id()].
all(#?MODULE{self = undefined,
             peers = Peers}) ->
    maps:keys(Peers);
all(#?MODULE{self = Self,
             peers = Peers}) ->
    [Self | maps:keys(Peers)].

-spec count(state()) -> non_neg_integer().
count(#?MODULE{peers = Peers}) ->
    maps:size(Peers) + 1.

-spec peer_ids(state()) -> [ra_server_id()].
peer_ids(#?MODULE{peers = Peers}) ->
    maps:keys(Peers).

-spec is_member(ra_server_id(), state()) ->
    boolean().
is_member(PeerId, #?MODULE{self = PeerId}) ->
    true;
is_member(PeerId, #?MODULE{peers = Peers}) ->
    maps:is_key(PeerId, Peers).

-spec add(ra_server_id(), state()) -> state().
add(PeerId, #?MODULE{peers = Peers} = State) ->
    case is_member(PeerId, State) of
        true ->
            State;
        false ->
            State#?MODULE{peers = maps:put(PeerId, #peer{}, Peers)}
    end.

-spec remove(ra_server_id(), state()) -> state().
remove(PeerId, #?MODULE{self = PeerId} = State) ->
    State#?MODULE{self = undefined};
remove(PeerId, #?MODULE{peers = Peers} = State) ->
    State#?MODULE{peers = maps:remove(PeerId, Peers)}.

-spec peer_match_indexes(state()) -> [ra_index()].
peer_match_indexes(#?MODULE{peers = Peers}) ->
    [M || #peer{match_index = M} <- maps:values(Peers)].

-spec match_and_next_index(ra_server_id(), state()) ->
    {ra_index(), ra_index()}.
match_and_next_index(PeerId, #?MODULE{peers = Peers}) ->
    #peer{match_index = MI, next_index = NI} = maps:get(PeerId, Peers),
    {MI, NI}.

-spec query_index(ra_server_id(), state()) -> ra_index().
query_index(PeerId, State) ->
    ?GET_PEER(PeerId, ?FUNCTION_NAME, State).

-spec match_index(ra_server_id(), state()) -> ra_index().
match_index(PeerId, State) ->
    ?GET_PEER(PeerId, ?FUNCTION_NAME, State).

-spec commit_index_sent(ra_server_id(), state()) -> ra_index().
commit_index_sent(PeerId, State) ->
    ?GET_PEER(PeerId, ?FUNCTION_NAME, State).

-spec next_index(ra_server_id(), state()) -> ra_index().
next_index(PeerId, State) ->
    ?GET_PEER(PeerId, ?FUNCTION_NAME, State).

-spec status(ra_server_id(), state()) -> peer_status().
status(PeerId, State) ->
    ?GET_PEER(PeerId, ?FUNCTION_NAME, State).

-spec set_next_index(ra_server_id(), ra_index(), state()) -> state().
set_next_index(PeerId, NextIdx, State) ->
    ?UPDATE_PEER(PeerId, next_index, NextIdx, State).

-spec set_match_index(ra_server_id(), ra_index(), state()) ->
    state().
set_match_index(PeerId, NextIdx, State) ->
    ?UPDATE_PEER(PeerId, match_index, NextIdx, State).

-spec set_commit_index(ra_server_id(), ra_index(), state()) ->
    state().
set_commit_index(PeerId, CommitIndex, State) ->
    ?UPDATE_PEER(PeerId, commit_index_sent, CommitIndex, State).

-spec set_query_index(ra_server_id(), ra_index(), state()) ->
    state().
set_query_index(PeerId, QueryIndex, State) ->
    ?UPDATE_PEER(PeerId, query_index, QueryIndex, State).

-spec set_status(ra_server_id(), peer_status(), state()) ->
    state().
set_status(PeerId, Status, State) ->
    ?UPDATE_PEER(PeerId, status, Status, State).

-spec set_match_and_next_index(ra_server_id(), ra_index(),
                               ra_index(), state()) -> state().
set_match_and_next_index(PeerId, MatchIdx, NextIdx, State) ->
    Peers0 = State#?MODULE.peers,
    Peer = maps:get(PeerId, Peers0),
    Peers = maps:put(PeerId, Peer#peer{match_index = MatchIdx,
                                       next_index = NextIdx}, Peers0),
    State#?MODULE{peers = Peers}.

-spec reset_peer(ra_server_id(), ra_index(), state()) -> state().
reset_peer(PeerId, NextIdx, State) ->
    Peers0 = State#?MODULE.peers,
    Peers = maps:put(PeerId, #peer{next_index = NextIdx}, Peers0),
    State#?MODULE{peers = Peers}.

-spec update_post_snapshot_installation(ra_server_id(), ra_index(), state()) ->
    state().
update_post_snapshot_installation(PeerId, Idx, State) ->
    Peers0 = State#?MODULE.peers,
    Peer = maps:get(PeerId, Peers0),
    Peers = maps:put(PeerId, Peer#peer{status = normal,
                                       match_index = Idx,
                                       next_index = Idx + 1}, Peers0),
    State#?MODULE{peers = Peers}.

-spec make_pipeline_rpc_effect(ra_index(), ra_index(), state()) ->
    [{pipeline_rpcs, [ra_server_id()]}].
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

-spec not_sending_snapshots(state()) ->
    [ra_server_id()].
not_sending_snapshots(#?MODULE{peers = Peers}) ->
    maps:keys(
      maps:filter(fun (_, #peer{status = {sending_snapshot, _}}) ->
                          false;
                      (_,  _) ->
                          % there are unconfirmed items
                          true
                  end, Peers)).

-spec stale(ra_index(), ra_index(), state()) -> [ra_server_id()].
stale(CommitIndex, NextLogIdx,  #?MODULE{peers = Peers}) ->
    maps:keys(
      maps:filter(fun (_, #peer{status = {sending_snapshot, _}}) ->
                          false;
                      (_, #peer{next_index = NI,
                                match_index = MI})
                        when MI < NI - 1 orelse NI < NextLogIdx ->
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
