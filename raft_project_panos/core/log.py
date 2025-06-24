class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

    def __repr__(self):
        return f"LogEntry(term={self.term}, command={self.command})"


class RaftLog:
    def __init__(self):
        self.entries = []

    def append_entry(self, term, command):
        entry = LogEntry(term, command)
        self.entries.append(entry)
        return len(self.entries) - 1  # index of new entry

    def get_entry(self, index):
        if 0 <= index < len(self.entries):
            return self.entries[index]
        return None

    def last_log_index(self):
        return len(self.entries) - 1

    def last_log_term(self):
        if self.entries:
            return self.entries[-1].term
        return 0

    def delete_entries_from(self, index):
        if 0 <= index < len(self.entries):
            self.entries = self.entries[:index]
#new
