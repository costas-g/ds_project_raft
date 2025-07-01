class Command:
    allowed_cmds = ['set', 'update', 'delete', 'no-op'] # add read?

    def __init__(self, cmd_type, key, value=None):
        if cmd_type not in self.allowed_cmds:
            raise ValueError(f"Invalid command type: {cmd_type}")
        self.cmd_type = cmd_type
        self.key = key
        self.value = value
        
    def __repr__(self):
        return f"Command({self.cmd_type}, {self.key}, {self.value})"

    def to_dict(self):
        return {
            "cmd_type": self.cmd_type,
            "key": self.key,
            "value": self.value
        }

    @staticmethod
    def from_dict(d):
        return Command(d['cmd_type'], d['key'], d.get('value'))
    
    
