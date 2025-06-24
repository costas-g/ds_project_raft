import time
import threading
from core.messages.append_entries import AppendEntries
from network.transport import send_message

class BatchingManager(threading.Thread):
    def __init__(self, leader_id, term, batch_interval=0.5):
        super().__init__()
        self.leader_id = leader_id
        self.term = term
        self.batch_interval = batch_interval
        self.queue = []
        self.running = True
        self.lock = threading.Lock()

    def add_command(self, command):
        with self.lock:
            self.queue.append(command)

    def run(self):
        while self.running:
            time.sleep(self.batch_interval)
            self.flush()

    def flush(self):
        with self.lock:
            if not self.queue:
                return

            batched = self.queue[:]
            self.queue.clear()

        print(f"[Batching] Sending batch of {len(batched)} commands from Leader {self.leader_id}")
        msg = AppendEntries(term=self.term, leader_id=self.leader_id, entries=batched)
        for receiver_id in range(1, 6):  # Example: assuming 5-node cluster
            if receiver_id != self.leader_id:
                send_message(receiver_id, msg)

    def stop(self):
        self.running = False
        self.flush()
