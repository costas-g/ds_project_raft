from typing import List # for linters
from raft.entry import Entry

class AppendEntries:
    def __init__(
        self, 
        term: int, 
        leader_id: str, 
        prev_log_index: int, 
        prev_log_term: int, 
        entries: List[Entry], 
        leader_commit: int
    ):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries   # List of Entry instances
        self.leader_commit = leader_commit
        
    def to_dict(self):
        return {
            'type': 'AppendEntries',
            'term': self.term,
            'leader_id': self.leader_id,
            'prev_log_index': self.prev_log_index,
            'prev_log_term': self.prev_log_term,
            'entries': [entry.to_dict() for entry in self.entries], # self.entries,
            'leader_commit': self.leader_commit
        }

    @staticmethod
    def from_dict(d):
        entries = [Entry.from_dict(e) for e in d['entries']]
        return AppendEntries(d['term'], d['leader_id'], d['prev_log_index'], d['prev_log_term'], entries, d['leader_commit'])

class AppendEntriesReply:
    def __init__(self, term: int, success: bool, last_log_index: int, source_id: str):
        self.term = term
        self.success = success
        self.last_log_index = last_log_index
        self.source_id = source_id

    def to_dict(self):
        return {
            'type': 'AppendEntriesReply',
            'term': self.term,
            'success': self.success,
            'last_log_index': self.last_log_index,
            'source_id': self.source_id
        }

    @staticmethod
    def from_dict(d):
        return AppendEntriesReply(
            d['term'],
            d['success'],
            d['last_log_index'],
            d['source_id']
        )
