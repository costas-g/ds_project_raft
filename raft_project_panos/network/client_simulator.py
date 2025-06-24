
import threading
import time
import random
from config import NODE_IDS
from queue import Queue

class ClientSimulator(threading.Thread):
    def __init__(self, client_id, leader_node, duration=5):
        super().__init__()
        self.client_id = client_id
        self.leader_node = leader_node  # reference to the leader Node instance
        self.duration = duration
        self.running = True

    def run(self):
        start_time = time.time()
        while self.running and (time.time() - start_time < self.duration):
            command = {
                "action": "set",
                "key": f"k{random.randint(1,10)}",
                "value": random.randint(100, 999)
            }
            print(f"[Client {self.client_id}] Sending command to Leader {self.leader_node.node_id}: {command}")
            if self.leader_node.batching:
                self.leader_node.batching.add_command(command)
            else:
                print(f"[Client {self.client_id}] Warning: Leader not ready for batching")
            time.sleep(random.uniform(0.5, 1.5))

    def stop(self):
        self.running = False
