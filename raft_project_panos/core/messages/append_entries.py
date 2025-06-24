from .base import RaftMessage

class AppendEntries(RaftMessage):
    def __init__(self, term, leader_id, entries=None, prev_log_index=0, prev_log_term=0, leader_commit=0):
        super().__init__(term, leader_id)
        self.entries = entries if entries is not None else []
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.leader_commit = leader_commit

class AppendEntriesResponse(RaftMessage):
    def __init__(self, term, sender_id, success):
        super().__init__(term, sender_id)
        self.success = success
