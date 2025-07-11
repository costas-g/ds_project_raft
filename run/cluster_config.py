NUM_NODES = 3  # Change to 5, 7, 9, etc.
MAX_NODES = 9

ALL_NODES = [f"n{i}" for i in range(1, MAX_NODES+1)]

localhost = '127.0.0.1'     # Only for same-machine communication
local_ip  = '127.0.0.1'     # The LAN IP address of the machine where the cluster runs on. Use this for access over the Internet when port forwarding is set up. 
public_ip = '127.0.0.1'     # The Public IP address for the clients to direct their requests

start_port_peer = 9000
start_port_client = 9100

nodes = ALL_NODES[:NUM_NODES]
addresses        =  {node: f"{localhost}:{start_port_peer   + i}" for i, node in enumerate(ALL_NODES)}  # for incoming messages from peers
lan_ip_addresses =  {node: f"{local_ip}:{start_port_client  + i}" for i, node in enumerate(ALL_NODES)}  # for incoming messages from clients
remote_addresses =  {node: f"{public_ip}:{start_port_client + i}" for i, node in enumerate(ALL_NODES)}  # for outgoing messages from clients
# client_ports = {node: start_port_client + i for i, node in enumerate(ALL_NODES)}

time_scale_factor = 1

timing = {
    "batching_interval":    time_scale_factor * 0.200,    #0.015
    "heartbeat_interval":   time_scale_factor * 0.500,    #0.075,
    "election_timeout_min": time_scale_factor * 1.000,    #0.150,
    "election_timeout_max": time_scale_factor * 2.000     #0.300
}

# Imported directly to the raft_node.py file
print_out = False       # for controlling print statements
CLOCK_PERIOD = 0.001    # for ticker
MAX_LOG = 2000          # maximum local log length