from raft.command import Command

class Entry:
    def __init__(self, term, command_id, command:Command):
        self.term = term
        self.command_id = command_id
        self.command = command  # instance of Command

    def __repr__(self):
        return f"Entry(term={self.term}, id={self.command_id}, command={self.command})"

    def to_dict(self):
        return {
            "term": self.term,
            "command_id": self.command_id,
            "command": self.command.to_dict()
        }

    @staticmethod
    def from_dict(d):
        command = Command.from_dict(d['command'])
        return Entry(d['term'], d['command_id'], command)