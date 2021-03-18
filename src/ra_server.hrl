%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-define(AER_CHUNK_SIZE, 128).
-define(FOLD_LOG_BATCH_SIZE, 25).
-define(DEFAULT_MAX_PIPELINE_COUNT, 4096).
-define(MAX_FETCH_ENTRIES, 4096).

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
         max_pipeline_count = ?DEFAULT_MAX_PIPELINE_COUNT :: non_neg_integer(),
         counter :: undefined | counters:counters_ref(),
         system_config :: ra_system:config()
        }).
