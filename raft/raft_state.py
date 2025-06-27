import json
import os

class RaftState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.state_file = f'state_{node_id}.json'
        self.load()

    def load(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                self.current_term = data.get('current_term', 0)
                self.voted_for = data.get('voted_for', None)
                self.log = data.get('log', [])

    def save(self):
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
        }
        with open(self.state_file, 'w') as f:
            json.dump(data, f)
