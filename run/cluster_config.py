NUM_NODES = 3  # Change to 5, 7, 9, etc.
MAX_NODES = 9

ALL_NODES = [f"n{i}" for i in range(1, MAX_NODES+1)]

nodes = ALL_NODES[:NUM_NODES]
addresses = {node: f"127.0.0.1:{9000 + i}" for i, node in enumerate(ALL_NODES)}
client_ports = {node: 9100 + i for i, node in enumerate(ALL_NODES)}

timing = {
    "heartbeat_interval":   1.0,    #0.15,
    "election_timeout_min": 3.0,    #0.3,
    "election_timeout_max": 6.0     #0.6
}
