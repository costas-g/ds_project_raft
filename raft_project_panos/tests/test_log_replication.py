import time
from queue import Queue
from core.node import Node
from network.client_simulator import ClientSimulator
from config import NODE_IDS


def test_log_replication():
    inboxes = {nid: Queue() for nid in NODE_IDS}
    nodes = [Node(nid, inboxes[nid]) for nid in NODE_IDS]

    for node in nodes:
        node.start()

    # Wait for leader election
    def wait_for_leader(timeout=5):
        start = time.time()
        while time.time() - start < timeout:
            for n in nodes:
                if n.role.name == "LEADER":
                    return n
            time.sleep(0.2)
        return None

    leader_node = wait_for_leader()
    assert leader_node is not None, "No leader was elected."
    print(f"[Test] Leader is Node {leader_node.node_id}")

    # Send a command to the leader
    command = {"action": "set", "key": "x", "value": 123}
    if leader_node.batching:
        leader_node.batching.add_command(command)
    else:
        print("[Test] Leader is not ready for batching")

    time.sleep(2)  # wait for replication

    for node in nodes:
        if node != leader_node:
            print(f"[Test] Verifying Node {node.node_id} log...")
            found = any(entry.command == command for entry in node.executor.state_machine.store.items())
            assert "x" in node.state_machine.store, f"Node {node.node_id} missing key 'x'"

    print("Test Passed: Log replicated successfully to all followers")

    for node in nodes:
        node.stop()
    for node in nodes:
        node.join()


if __name__ == "__main__":
    test_log_replication()
