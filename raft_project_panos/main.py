from queue import Queue
import time
from core.node import Node
from network.client_simulator import ClientSimulator
from config import NODE_IDS

# Δημιουργία queues και κόμβων
inboxes = {node_id: Queue() for node_id in NODE_IDS}
nodes = [Node(node_id, inboxes[node_id]) for node_id in NODE_IDS]

# Εκκίνηση κόμβων
for node in nodes:
    node.start()

# Αναμονή έως ότου εκλεγεί Leader
def wait_for_leader(timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        for node in nodes:
            if node.role.name == "LEADER":
                return node
        time.sleep(0.2)
    return None

leader_node = wait_for_leader()
if leader_node:
    print(f"[Main] Detected Leader: Node {leader_node.node_id}")
else:
    print("[Main] No leader elected in time. Exiting.")
    for node in nodes:
        node.stop()
    for node in nodes:
        node.join()
    exit(1)

# Εκκίνηση client που στέλνει commands μέσω του batching του Leader
client = ClientSimulator(client_id=1, leader_node=leader_node, duration=6)
client.start()

try:
    time.sleep(10)
finally:
    client.stop()
    client.join()

    for node in nodes:
        node.stop()
    for node in nodes:
        node.join()

    print("Cluster and client shut down cleanly.")
