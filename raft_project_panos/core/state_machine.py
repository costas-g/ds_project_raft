class StateMachine:
    def __init__(self):
        self.store = {}  # απλό key-value store

    def apply(self, command):
        """
        Εφαρμόζει μια εντολή τύπου dict, π.χ. {"action": "set", "key": "x", "value": 42}
        """
        if command["action"] == "set":
            self.store[command["key"]] = command["value"]
        elif command["action"] == "delete":
            self.store.pop(command["key"], None)

    def get(self, key):
        return self.store.get(key)

    def __repr__(self):
        return f"StateMachine({self.store})"
