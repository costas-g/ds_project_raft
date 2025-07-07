#key value store  for create, read, update, delete
import threading    # using threads in case 
                    # multiple threads apply entries to the store
from raft.command import Command

class StateMachine:
    def __init__(self):
        self.store = {}
        self.lock = threading.Lock()

    def apply(self, command: Command):
        if command.cmd_type == 'no-op':
            return  # No effect
        
        cmd = command.cmd_type
        key = command.key
        value = command.value
        
        
        with self.lock:  
            if cmd == "create":
                if key in self.store:
                    return f"Key '{key}' already exists."
                else:
                    self.store[key] = value
                    return f"Created and set {key} = {value}"
            elif cmd == "read":
                if key in self.store:
                    read_value = self.store[key]
                    return f"Read {key} = {read_value}"
                else:
                    return f"{key} not found for read"  
            elif cmd == "update":
                if key in self.store:
                    self.store[key] = value
                    return f"Updated {key} = {value}"
                else:
                    return f"{key} not found for update"   
            elif cmd == "delete":
                if key in self.store:
                    del self.store[key]
                    return f"Deleted {key}"
                else:
                    return f"{key} not found for delete" 
                # popped = self.store.pop(key, None)
                # return f"Deleted {key}" if popped is not None else f"{key} not found"
            else:
                return f"Invalid command: {cmd}"
            
    def load_snapshot(self, snapshot_data: dict):
        self.store = snapshot_data.copy()  # assumes self.store is the internal KV state

    def get_snapshot(self) -> dict:
        '''Return a shallow copy of the current state'''
        return self.store.copy()

    def get(self, key):
        return self.store.get(key)

    def __repr__(self):
        return f"StateMachine({self.store})"

    def dump(self):
        return dict(self.store)  # Return a copy - Essentially the same as get_snapshot