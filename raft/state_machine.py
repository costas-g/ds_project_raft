#key value store  for set , delete , update 
import threading    # using threads in case 
                    # multiple threads apply entries to the store
from command import Command

class StateMachine:
    def __init__(self):
        self.store = {}
        self.lock = threading.Lock()

    def apply(self, command: Command):
        cmd = command.get("cmd_type")
        key = command.get("key")
        value = command.get("value")
        
        
        with self.lock:  
            if cmd == "set":
                self.store[key] = value
                return f"Set {key} = {value}"
            elif cmd == "delete":
                popped = self.store.pop(key, None)
                return f"Deleted {key}" if popped is not None else f"{key} not found"
            elif cmd == "update":
                if key in self.store:
                    self.store[key] = value
                    return f"Updated {key} = {value}"
                else:
                    return f"{key} not found for update"                
            else:
                return f"Unknown cmd: {cmd}"

    def get(self, key):
        return self.store.get(key)

    def __repr__(self):
        return f"StateMachine({self.store})"
