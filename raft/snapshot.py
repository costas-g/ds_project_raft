import os
import json
from typing import Dict

class Snapshot:
    def __init__(self, node_id: str, state_dir: str = 'states'):
        self.node_id = node_id
        self.state_dir = state_dir
        # os.makedirs(self.state_dir, exist_ok=True) # this is already called in RaftState, so it might be redundant

        # metadata
        self.last_included_index: int = -1
        self.last_included_term: int = 0
        # data - state machine state
        self.data: Dict = {}

    @property
    def snapshot_file(self) -> str:
        return os.path.join(self.state_dir, f'snapshot_{self.node_id}.json')

    def load(self) -> None:
        '''Reads data from snapshot file, if it exists, and loads them to memory. '''
        if os.path.exists(self.snapshot_file):
            with open(self.snapshot_file, 'r') as f:
                content = json.load(f)
                self.last_included_index = content.get('last_included_index', -1)
                self.last_included_term = content.get('last_included_term', 0)
                self.data = content.get('snapshot', {})
        else:
            self.last_included_index = -1
            self.last_included_term = 0
            self.data = {}

    def save(self) -> None:
        '''Saves snapshot data to file (Write to disk)'''
        content = {
            'last_included_index': self.last_included_index,
            'last_included_term': self.last_included_term,
            'snapshot': self.data
        }
        with open(self.snapshot_file, 'w') as f:
            json.dump(content, f)

    def update(self, last_included_index: int, last_included_term: int, data: Dict) -> None:
        '''Updates in-memory snapshot attributes (No write to disk)'''
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.data = data
        # self.save() # No write to disk for more control over writes
