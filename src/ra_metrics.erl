-module(ra_metrics).

%% What externally accessible metrics do we want?
%%
%% commit_index, last_index, last_written,
%%
%% raft_state: leader, follower, etc
%%
%% - Snapshot rate?
%% - Log size (from snapshot to last written) bytes and entries
%% - entries in memory and in segments
%% - Commit latency - use timestamps? What storage?

-export([
         ]).

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-define(GETTER(State), State#?MODULE.?FUNCTION_NAME).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
