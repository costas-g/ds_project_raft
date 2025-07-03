import json
import os
from typing import List
from raft.entry import Entry  # adjust import path as needed

class RaftState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log: List[Entry] = []
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
        '''Returns this node's last lost index (-1 if log is empty)'''
        return len(self.log) - 1

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
        
    # helper for backtracking fast   
    def get_term_first_index(self, term: int, high_index: int = None) -> int | None:
        '''Returns the first index of the given term in the log using binary search for efficiency'''
        low = 0
        high = high_index if high_index is not None else len(self.log) - 1
        result = None
        while low <= high:
            mid = (low + high) // 2
            mid_term = self.log[mid].term
            if mid_term == term:
                result = mid
                high = mid - 1  # look left for earlier occurrence
            elif mid_term < term:
                low = mid + 1
            else:
                high = mid - 1
        return result
