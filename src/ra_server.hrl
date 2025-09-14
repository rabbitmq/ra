%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
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
         metrics_labels :: seshat:labels_map(),
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
