import json
import os
from raft.entry import Entry  # adjust import path as needed

class RaftState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.state_dir = 'states'
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f'state_{node_id}.json')
        self.load()

    def load(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                self.current_term = data.get('current_term', 0)
                self.voted_for = data.get('voted_for', None)
                self.log = [Entry.from_dict(e) for e in data.get('log', [])] # self.log = data.get('log', [])

    def save(self):
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': [e.to_dict() for e in self.log] # 'log': self.log,
        }
        with open(self.state_file, 'w') as f:
            json.dump(data, f)

    # helper methods for granting votes
    def get_last_log_index(self):
        return len(self.log) - 1    # returns -1 if log is empty

    def get_last_log_term(self):
        if not self.log:
            return 0                # returns 0 if log is empty
        return self.log[-1].term
    
    def is_log_up_to_date(self, candidate_last_index, candidate_last_term):
        # if not self.log:
        #     return True  # Empty log is always up-to-date

        my_last_index = self.get_last_log_index()
        my_last_term = self.get_last_log_term()

        if candidate_last_term > my_last_term:
            return True
        elif candidate_last_term == my_last_term:
            return candidate_last_index >= my_last_index
        else:
            return False
