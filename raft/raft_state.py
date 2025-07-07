import json
import os
from typing import List
from raft.entry import Entry  # adjust import path as needed
from raft.snapshot import Snapshot 

class RaftState:
    '''Contains persistent state, log, and snapshot'''
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None

        self.log: List[Entry] = []

        self.state_dir = 'states'
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f'state_{node_id}.json')
        self.log_file = os.path.join(self.state_dir, f'log_{node_id}.ndjson')

        # Snapshot 
        self.snapshot_file = os.path.join(self.state_dir, f'snapshot_{self.node_id}.json')
        self.snapshot = Snapshot(node_id)

        self.load()
        self.snapshot.load()
        self.log_start_index = self.snapshot.last_included_index + 1

    def load(self):
        '''Load the persistent state and the log'''
        # Load the state file
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                self.current_term = data.get('current_term', 0)
                self.voted_for = data.get('voted_for', None)

                # Optionally load snapshot data if saved separately
        
        # Load the log file
        if os.path.exists(self.log_file):
            self.log = []
            with open(self.log_file, 'r') as f:
                for line in f:
                    entry_dict = json.loads(line.strip())
                    self.log.append(Entry.from_dict(entry_dict))
                # data = json.load(f)
                # self.log = [Entry.from_dict(e) for e in data.get('log', [])] # self.log = data.get('log', [])

    def save(self):
        '''Save persistent state (current_term and voted_for) to file (Write to disk)'''
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for
            #'log': [e.to_dict() for e in self.log] # 'log': self.log
            # Snapshot data could be saved separately if large            
        }
        with open(self.state_file, 'w') as f:
            json.dump(data, f)
    
    # helper methods for modifying and saving the log
    def append_log_entry_to_file(self, entry: Entry):
        '''Writes the entry to file in disk in append mode.'''
        with open(self.log_file, "a") as f:
            f.write(json.dumps(entry.to_dict()) + "\n")

    def append_log_entries_to_file(self, entries: list[Entry]):
        '''Writes the entry to file in disk in append mode.'''
        with open(self.log_file, "a") as f:
            for entry in entries:
                f.write(json.dumps(entry.to_dict()) + "\n")

    def save_log_to_file(self, log_list: List[Entry]):
        '''Rewrites the whole log to file in disk'''
        with open(self.log_file, "w") as f:
            for entry in log_list:
                f.write(json.dumps(entry.to_dict()) + "\n")

    # helper methods for snapshot support  
    def global_to_local(self, global_index: int) -> int:
        """Convert a global log index to a local index into self.log."""
        return global_index - self.log_start_index

    def local_to_global(self, local_index: int) -> int:
        """Convert a local index in self.log to the global log index."""
        return self.log_start_index + local_index

    def log_truncate_suffix(self, global_index: int):
        """Keeps log entries up to (but not including) global index. (No write to disk)"""
        local_index = self.global_to_local(global_index) # global_index - self.log_start_index
        if local_index < 0:
            # Trying to truncate at negative local_index 
            # print(f'WARNING: truncating log suffix at negative local_index (given global_index: {global_index}, self.log_start_index: {self.log_start_index}), log_length: {len(self.log)}. Log will now be empty.')
            self.log = []
        else:
            self.log = self.log[:local_index] # Works even when index is OOB, so the if-else might be redundant

    def log_truncate_prefix(self, global_index: int):
        """Keeps log entries after the specified global index. Also updates the log_start_index. (No write to disk)"""
        local_index = self.global_to_local(global_index) # global_index - self.log_start_index
        if local_index >= len(self.log):
            # Trying to truncate after last_index
            # print(f'WARNING: truncating log prefix after local last index (given global_index: {global_index}, self.log_start_index: {self.log_start_index}, log_length: {len(self.log)}). Log will now be empty.')
            self.log = []
        else:
            self.log = self.log[local_index+1:] # Works even when index is OOB, so the if-else might be redundant
        # update log_start_index
        self.log_start_index = global_index + 1
    
    def install_snapshot(self, last_included_index: int, last_included_term: int, snapshot_data: dict):
        '''Updates snapshot and truncates log to memory, then saves to files (Write to disk)'''
        # update snapshot variables (in memory)
        self.snapshot.update(last_included_index, last_included_term, snapshot_data)

        # Save snapshot_data separately if needed
        self.snapshot.save()

        # Discard previous log entries
        self.log_truncate_prefix(last_included_index)
        # Rewrite log file to reflect truncation
        self.save_log_to_file(self.log)

        # Save persistent state
        self.save()

    def get_log_entry(self, global_index: int) -> Entry | None:
        local_index = self.local_to_global(global_index) 
        if local_index < 0 or local_index >= len(self.log):
            return None
        return self.log[local_index]
    
    # helper methods for granting votes - updated for snapshot support
    def get_last_log_index(self):
        '''Returns this node's last log (global) index (-1 if no entries have ever been appended)'''
        return self.log_start_index + len(self.log) - 1

    def get_last_log_term(self): 
        if not self.log:
            return self.snapshot.last_included_term
        return self.log[-1].term
    
    def get_term_at_index(self, global_index: int) -> int:
        '''Returns term at given global index. Raises error if the entry at global index has been discarded.'''
        if global_index == self.snapshot.last_included_index:
            return self.snapshot.last_included_term
        if global_index < self.log_start_index:
            raise IndexError("Index is before start of log and not in snapshot.")
        local_index = self.global_to_local(global_index)
        if local_index >= len(self.log):
            raise IndexError("Index out of bounds.")
        return self.log[local_index].term
    
    def is_log_up_to_date(self, candidate_last_index, candidate_last_term): ###################################
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
        '''Returns the first index of the given term in the log using binary search for efficiency. If not found, returns None.
        \nhigh_index should be a global_index'''
        low = 0
        high = self.global_to_local(high_index) if high_index is not None else len(self.log) - 1
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
        return result + self.log_start_index if result is not None else None
