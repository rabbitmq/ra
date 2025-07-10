%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-define(AER_CHUNK_SIZE, 128).
-define(DEFAULT_MAX_PIPELINE_COUNT, 4096).
-define(DEFAULT_SNAPSHOT_CHUNK_SIZE, 1000000). % 1MB
-define(DEFAULT_RECEIVE_SNAPSHOT_TIMEOUT, 30000).
-define(DEFAULT_MACHINE_UPGRADE_STRATEGY, all).
-define(FLUSH_COMMANDS_SIZE, 16).

-record(cfg,
        {id :: ra_server_id(),
         uid :: ra_uid(),
         log_id :: unicode:chardata(),
         metrics_key :: term(),
         machine :: ra_machine:machine(),
         machine_version :: ra_machine:version(),
         machine_versions :: [{ra_index(), ra_machine:version()}, ...],
         effective_machine_version :: ra_machine:version(),
         effective_machine_module :: module(),
         effective_handle_aux_fun :: undefined | {handle_aux, 5 | 6},
         max_pipeline_count = ?DEFAULT_MAX_PIPELINE_COUNT :: non_neg_integer(),
         max_append_entries_rpc_batch_size = ?AER_CHUNK_SIZE :: non_neg_integer(),
         counter :: undefined | counters:counters_ref(),
         system_config :: ra_system:config()
        }).

-record(ra_server_state,
        {cfg :: #cfg{},
         leader_id :: term(),
         cluster :: term(),
         cluster_change_permitted :: boolean(),
         cluster_index_term :: term(),
         previous_cluster :: term(),
         current_term :: term(),
         log :: term(),
         voted_for :: term(),
         votes :: non_neg_integer(),
         membership :: term(),
         commit_index :: term(),
         last_applied :: term(),
         persisted_last_applied :: term(),
         stop_after :: term(),
         machine_state :: term(),
         aux_state :: term(),
         condition :: term(),
         pre_vote_token :: reference(),
         query_index :: non_neg_integer(),
         queries_waiting_heartbeats :: term(),
         pending_consistent_queries :: term(),
         commit_latency :: term()
        }).
