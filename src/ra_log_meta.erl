%% @hidden
-module(ra_log_meta).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1,
         store/3,
         store_sync/3,
         delete/1,
         delete_sync/1,
         fetch/2,
         fetch/3
        ]).

-include("ra.hrl").

%% centralised meta data storage server for ra servers.

-type key() :: current_term | voted_for | last_applied.
-type value() :: non_neg_integer() | atom() | {atom(), atom()}.

-define(TBL_NAME, ?MODULE).
-define(TIMEOUT, 30000).
-define(SYNC_INTERVAL, 5).

-record(?MODULE, {ref :: reference()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec init(file:filename()) -> {ok, state()}.
init(Dir) ->
    process_flag(trap_exit, true),
    MetaFile = filename:join(Dir, "meta.dets"),
    ok = filelib:ensure_dir(MetaFile),
    {ok, Ref} = dets:open_file(?TBL_NAME, [{file, MetaFile},
                                           {auto_save, ?SYNC_INTERVAL}]),
    _ = ets:new(?TBL_NAME, [named_table, public, {read_concurrency, true}]),
    ?TBL_NAME = dets:to_ets(?TBL_NAME, ?TBL_NAME),
    ?INFO("ra: meta data store initialised. ~b record(s) recovered",
          [ets:info(?TBL_NAME, size)]),
    {ok, #?MODULE{ref = Ref}}.

handle_batch(Commands, #?MODULE{ref = Ref} = State) ->
    DoInsert =
        fun (Id, Key, Value, Inserts0) ->
                case Inserts0 of
                    #{Id := Data} ->
                        Inserts0#{Id => update_key(Key, Value, Data)};
                    _ ->
                        case dets:lookup(Ref, Id) of
                            [{Id, T, V, A}] ->
                                Data = {Id, T, V, A},
                                Inserts0#{Id => update_key(Key, Value, Data)};
                            [] ->
                                Data = {Id, undefined, undefined, undefined},
                                Inserts0#{Id => update_key(Key, Value, Data)}
                        end
                end
        end,
    {Inserts, Replies, ShouldSync} =
        lists:foldl(
          fun ({cast, {store, Id, Key, Value}},
               {Inserts0, Replies, DoSync}) ->
                  {DoInsert(Id, Key, Value, Inserts0), Replies, DoSync};
              ({call, From, {store, Id, Key, Value}},
               {Inserts0, Replies, _DoSync}) ->
                  {DoInsert(Id, Key, Value, Inserts0),
                   [{reply, From, ok} | Replies], true};
              ({cast, {delete, Id}},
               {Inserts0, Replies, DoSync}) ->
                  {handle_delete(Id, Ref, Inserts0), Replies, DoSync};
              ({call, From, {delete, Id}},
               {Inserts0, Replies, _DoSync}) ->
                  {handle_delete(Id, Ref, Inserts0),
                   [{reply, From, ok} | Replies], true}
          end, {#{}, [], false}, Commands),
    Objects = maps:values(Inserts),
    ok = dets:insert(?MODULE, Objects),
    true = ets:insert(?MODULE, Objects),
    case ShouldSync of
        true ->
            ok = dets:sync(?MODULE);
        false ->
            ok
    end,
    {ok, Replies, State}.

terminate(_, #?MODULE{ref = Ref}) ->
    ok = dets:sync(?MODULE),
    _ = dets:close(Ref),
    ok.

format_status(State) ->
    State.

%% send a message to the meta data store using cast
-spec store(ra_uid(), key(), value()) -> ok.
store(UId, Key, Value) ->
    gen_batch_server:cast(?MODULE, {store, UId, Key, Value}).

%% waits until batch has been processed and synced.
%% when it returns the store request has been safely flushed to disk
-spec store_sync(ra_uid(), key(), value()) -> ok.
store_sync(UId, Key, Value) ->
    gen_batch_server:call(?MODULE, {store, UId, Key, Value}, ?TIMEOUT).

-spec delete(ra_uid()) -> ok.
delete(UId) ->
    gen_batch_server:cast(?MODULE, {delete, UId}).

-spec delete_sync(ra_uid()) -> ok.
delete_sync(UId) ->
    gen_batch_server:call(?MODULE, {delete, UId}, ?TIMEOUT).

%% READER API

-spec fetch(ra_uid(), key()) -> value() | undefined.
fetch(Id, current_term) ->
    maybe_fetch(Id, 2);
fetch(Id, voted_for) ->
    maybe_fetch(Id, 3);
fetch(Id, last_applied) ->
    maybe_fetch(Id, 4).

-spec fetch(ra_uid(), key(), term()) -> value().
fetch(Id, Key, Default) ->
    case fetch(Id, Key) of
        undefined -> Default;
        Value -> Value
    end.

%%% internal

maybe_fetch(Id, Pos) ->
    try ets:lookup_element(?TBL_NAME, Id, Pos) of
        E -> E
    catch
        _:badarg ->
            undefined
    end.

handle_delete(Id, Ref, Inserts) ->
  _ = dets:delete(Ref, Id),
  _ = ets:delete(?MODULE, Id),
  maps:remove(Id, Inserts).

update_key(current_term, Value, Data) ->
    setelement(2, Data, Value);
update_key(voted_for, Value, Data) ->
    setelement(3, Data, Value);
update_key(last_applied, Value, Data) ->
    setelement(4, Data, Value).
