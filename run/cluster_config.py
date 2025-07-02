NUM_NODES = 3  # Change to 5, 7, 9, etc.
MAX_NODES = 9

ALL_NODES = [f"n{i}" for i in range(1, MAX_NODES+1)]

nodes = ALL_NODES[:NUM_NODES]
addresses = {node: f"127.0.0.1:{9000 + i}" for i, node in enumerate(ALL_NODES)}
client_ports = {node: 9100 + i for i, node in enumerate(ALL_NODES)}

time_scale_factor = 20

timing = {
    "batching_interval":    time_scale_factor * 0.015,    #0.015
    "heartbeat_interval":   time_scale_factor * 0.075,    #0.075,
    "election_timeout_min": time_scale_factor * 0.150,    #0.150,
    "election_timeout_max": time_scale_factor * 0.300     #0.300
}
