{application, 'ra', [
	{description, "Experimental raft library"},
	{vsn, "0.1.0"},
	{modules, ['ra','ra_app','ra_directory','ra_env','ra_fifo','ra_fifo_index','ra_heartbeat_monitor','ra_lib','ra_log','ra_log_file','ra_log_file_ets','ra_log_file_meta','ra_log_file_segment','ra_log_file_segment_writer','ra_log_file_snapshot_writer','ra_log_file_sup','ra_log_memory','ra_log_wal','ra_log_wal_sup','ra_lru','ra_machine','ra_metrics_ets','ra_node','ra_node_proc','ra_nodes_sup','ra_proxy','ra_sup','ra_system_sup']},
	{registered, [ra_sup]},
	{applications, [kernel,stdlib,sasl,crypto]},
	{mod, {ra_app, []}},
	{env, [
	{data_dir, "/var/vcap/store/ra/shared"}
]}
]}.