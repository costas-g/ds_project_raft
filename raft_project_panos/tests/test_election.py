import time
from queue import Queue
from core.node import Node
from config import NODE_IDS


def test_leader_election():
    inboxes = {nid: Queue() for nid in NODE_IDS}
    nodes = [Node(nid, inboxes[nid]) for nid in NODE_IDS]

    for node in nodes:
        node.start()

    time.sleep(5)  # Χρόνος για να πραγματοποιηθεί election

    leaders = [n for n in nodes if n.role.name == "LEADER"]
    assert len(leaders) == 1, f"Expected 1 leader, found {len(leaders)}"
    print(f"Test Passed: Node {leaders[0].node_id} is the leader")

    for node in nodes:
        node.stop()
    for node in nodes:
        node.join()


if __name__ == "__main__":
    test_leader_election()
