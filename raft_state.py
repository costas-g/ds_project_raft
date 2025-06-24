class RaftState:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.log = []  # list of log entries, each entry can be a dict with term and command_id

    def save_to_file(self, filename):
        import json
        with open(filename, 'w') as f:
            json.dump({
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'log': self.log,
            }, f, indent = 2)

    def load_from_file(self, filename):
        import json
        with open(filename) as f:
            data = json.load(f)
            self.current_term = data['current_term']
            self.voted_for = data['voted_for']
            self.log = data['log']
