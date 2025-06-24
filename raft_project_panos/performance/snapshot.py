import pickle
import os

class SnapshotManager:
    def __init__(self, node_id, state_machine, log, snapshot_dir="snapshots"):
        self.node_id = node_id
        self.state_machine = state_machine
        self.log = log
        self.snapshot_dir = snapshot_dir
        os.makedirs(snapshot_dir, exist_ok=True)

    def create_snapshot(self):
        snapshot_data = {
            "store": self.state_machine.store,
            "log_index": len(self.log.entries)
        }
        path = os.path.join(self.snapshot_dir, f"snapshot_{self.node_id}.bin")
        with open(path, "wb") as f:
            pickle.dump(snapshot_data, f)
        print(f"[Snapshot] Node {self.node_id} saved snapshot at log index {snapshot_data['log_index']}")

    def load_snapshot(self):
        path = os.path.join(self.snapshot_dir, f"snapshot_{self.node_id}.bin")
        if not os.path.exists(path):
            print(f"[Snapshot] No snapshot found for Node {self.node_id}")
            return False

        with open(path, "rb") as f:
            snapshot_data = pickle.load(f)

        self.state_machine.store = snapshot_data["store"]
        self.log.entries = []  # clear log for simplicity
        print(f"[Snapshot] Node {self.node_id} restored snapshot with keys: {list(self.state_machine.store.keys())}")
        return True
