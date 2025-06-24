import time
from queue import Queue
from core.node import Node
from network.client_simulator import ClientSimulator
from config import NODE_IDS


def benchmark_cluster(runtime=10, client_count=5):
    inboxes = {nid: Queue() for nid in NODE_IDS}
    nodes = [Node(nid, inboxes[nid]) for nid in NODE_IDS]

    for node in nodes:
        node.start()

    # Αναμονή για election
    def wait_for_leader(timeout=5):
        start = time.time()
        while time.time() - start < timeout:
            for n in nodes:
                if n.role.name == "LEADER":
                    return n
            time.sleep(0.2)
        return None

    leader_node = wait_for_leader()
    if not leader_node:
        print("[Benchmark] No leader elected. Aborting benchmark.")
        return

    print(f"[Benchmark] Leader is Node {leader_node.node_id}")

    # Εκκίνηση πολλών clients
    clients = [ClientSimulator(i, leader_node, duration=runtime) for i in range(client_count)]
    for c in clients:
        c.start()

    start_time = time.time()
    time.sleep(runtime)
    end_time = time.time()

    for c in clients:
        c.stop()
    for c in clients:
        c.join()

    for n in nodes:
        n.stop()
    for n in nodes:
        n.join()

    print(f"[Benchmark] Ran for {end_time - start_time:.2f}s with {client_count} clients")


if __name__ == "__main__":
    benchmark_cluster(runtime=10, client_count=5)
