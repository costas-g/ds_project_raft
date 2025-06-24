import threading
from collections import defaultdict
from queue import Queue

class ParallelExecutor:
    def __init__(self, state_machine):
        self.state_machine = state_machine
        self.queues = defaultdict(Queue)
        self.threads = {}
        self.running = True

    def start(self):
        for key in self.queues:
            if key not in self.threads:
                thread = threading.Thread(target=self._worker, args=(key,), daemon=True)
                thread.start()
                self.threads[key] = thread

    def _worker(self, key):
        queue = self.queues[key]
        while self.running:
            try:
                command = queue.get(timeout=1)
                self.state_machine.apply(command)
            except:
                continue

    def submit(self, command):
        key = command.get("key")
        if key:
            self.queues[key].put(command)
            self.start()  # Ensure the thread is running

    def stop(self):
        self.running = False
        for thread in self.threads.values():
            thread.join(timeout=1)
