-module(ra_kv).
-behaviour(ra_machine).
-include("src/ra.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([
         init/1,
         apply/3,
         % state_enter/2,
         % tick/2,
         init_aux/1,
         handle_aux/5,
         live_indexes/1,
         overview/1
        ]).

-export([
         start_cluster/3,
         add_member/3,
         member_overview/1,

         put/4,
         get/3,
         query_get/3,
         take_snapshot/1
        ]).


-define(STATE, ?MODULE).
-define(TUPLE(A, B), [A | B]).

-type key() :: binary().
-type value() :: term().

-record(?STATE, {keys = #{} ::
                 #{key() => ?TUPLE(non_neg_integer(), Hash :: integer())}}).


-record(put, {key :: key(),
              value :: term(),
              meta :: #{size := non_neg_integer(),
                        hash := integer()}}).

-type command() :: #put{}.
-opaque state() :: #?STATE{}.

-export_type([state/0,
              command/0]).

%% mgmt
-spec start_cluster(System :: atom(),
                    ClusterName :: atom(),
                    Config :: #{members := [ra_server_id()]}) ->
    {ok, [ra_server_id()], [ra_server_id()]} |
    {error, cluster_not_formed}.
start_cluster(System, ClusterName, #{members := ServerIds})
  when is_atom(ClusterName) andalso
       is_atom(System) ->
    Machine = {module, ?MODULE, #{}},
    Configs = [begin
                   UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
                   #{id => Id,
                     uid => UId,
                     cluster_name => ClusterName,
                     log_init_args => #{uid => UId,
                                        min_snapshot_interval => 0},
                     initial_members => ServerIds,
                     machine => Machine}
               end || Id <- ServerIds],
    ra:start_cluster(System, Configs).

add_member(System, {Name, _} = Id, LeaderId) ->
    {ok, Members, _} = ra:members(LeaderId),
    UId = ra:new_uid(ra_lib:to_binary(Name)),
    Machine = {module, ?MODULE, #{}},
    Config = #{id => Id,
               uid => UId,
               cluster_name => Name,
               log_init_args => #{uid => UId,
                                  min_snapshot_interval => 0},
               initial_members => Members,
               machine => Machine},
    ok = ra:start_server(System, Config),
    {ok, _, _} =  ra:add_member(LeaderId, Id),
    ok.

member_overview(ServerId) ->
    case ra:member_overview(ServerId) of
        {ok, O, _} ->
            maps:with([log, machine], O);
        Err ->
            Err
    end.


%% client
-spec put(ra:server_id(), key(), value(), non_neg_integer()) ->
    {ok, map()} | {error, term()} | {timeout, ra:server_id()}.
put(ServerId, Key, Value, Timeout) ->
    Hash = erlang:phash2(Value),
    Put = #put{key = Key,
               value = Value,
               meta = #{size => erlang:external_size(Value),
                        hash => Hash}},
    case ra:process_command(ServerId, Put, Timeout) of
        {ok, {ok, Meta}, LeaderId} ->
            {ok, Meta#{leader => LeaderId}};
        Err ->
            Err
    end.


%% @doc get performs a consistent query that returns the index, hash and member set
%% then perform an aux query to actually get the data for a given index.
%% if addressing a follower (say there is a local one) then the read may need
%% to wait if the index isn't yet available locally (term also need to be checked)
%% or check that the machien state has the right index for a given key before
%% reading the value from the log
-spec get(ra:server_id(), key(), non_neg_integer()) ->
    {ok, map(), value()} | {error, term()} | {timeout, ra:server_id()}.
get(ServerId, Key, Timeout) ->
    case ra:consistent_query(ServerId, {?MODULE, query_get,
                                        [element(1, ServerId), Key]}, Timeout) of
        {ok, {ok, Idx, Members}, LeaderId} ->
            case ra_server_proc:read_entries(LeaderId, [Idx],
                                             undefined, Timeout) of
                {ok, {#{Idx := {Idx, Term,
                                {'$usr', Meta, #put{value = Value}, _}}}, Flru}} ->
                    _ = ra_flru:evict_all(Flru),
                    {ok, Meta#{index => Idx,
                               members => Members,
                               term => Term}, Value};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.


query_get(ClusterName, Key, #?STATE{keys = Keys}) ->
    Members = ra_leaderboard:lookup_members(ClusterName),
    case Keys of
        #{Key := [Idx |_]} ->
            {ok, Idx, Members};
        _ ->
            {error, not_found}
    end.

-spec take_snapshot(ra_server_id()) -> ok.
take_snapshot(ServerId) ->
    ra:aux_command(ServerId, take_snapshot).

%% state machine

init(_) ->
    #?MODULE{}.

%% we use improper lists in this module
-dialyzer({no_improper_lists, [apply/3]}).

apply(#{index := Idx} = Meta,
      #put{key = Key,
           meta = #{hash := Hash}},
      #?STATE{keys = Keys} = State0) ->
    State = State0#?STATE{keys = Keys#{Key => ?TUPLE(Idx, Hash)}},
    {State, {ok, Meta}, []}.

live_indexes(#?STATE{keys = Keys}) ->
    maps:fold(fun (_K, [Idx | _], Acc) ->
                      [Idx | Acc]
              end, [], Keys).


-record(aux, {}).

init_aux(_) ->
    #aux{}.

handle_aux(_RaState, {call, _From}, take_snapshot, Aux, Internal) ->
    MacState = ra_aux:machine_state(Internal),
    LastAppliedIdx = ra_aux:last_applied(Internal),
    %% TODO: replace release cursor with simpler snapshot effect that is always
    %% attempted?
    {reply, ok, Aux, Internal,
     [{release_cursor, LastAppliedIdx, MacState}]};
handle_aux(_RaState, _, _, Aux, Internal) ->
    {no_reply, Aux, Internal}.

overview(#?STATE{keys = Keys} = State) ->
    #{num_keys => maps:size(Keys),
      live_indexes => live_indexes(State)}.
