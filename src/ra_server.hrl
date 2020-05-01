-define(AER_CHUNK_SIZE, 25).
-define(FOLD_LOG_BATCH_SIZE, 25).
% TODO: test what is a good default here
% TODO: make configurable
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
         max_pipeline_count = ?DEFAULT_MAX_PIPELINE_COUNT :: non_neg_integer()
        }).
