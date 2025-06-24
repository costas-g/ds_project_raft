import json

class AppendEntries:
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        self.term = term                        # leader’s term
        self.leader_id = leader_id              # leader’s ID
        self.prev_log_index = prev_log_index    # index of log entry immediately preceding new ones
        self.prev_log_term = prev_log_term      # term of prev_log_index entry
        self.entries = entries or []            # list of new log entries (can be empty for heartbeat)
        self.leader_commit = leader_commit      # leader’s commit index

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return AppendEntries(**obj)


class AppendEntriesReply:
    def __init__(self, term, success):
        self.term = term
        self.success = success  # True if follower appended entries successfully

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return AppendEntriesReply(**obj)
