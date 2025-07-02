from typing import List # for linters
from raft.command import Command

class ClientRequest:
    def __init__(
        self, 
        client_id: str, 
        command_id: int,
        command: Command
    ):
        self.client_id = client_id
        self.command_id = command_id
        self.command = command
        
    def to_dict(self):
        return {
            'type': 'ClientRequest',
            'client_id': self.client_id,
            'command_id': self.command_id,
            'command': self.command.to_dict()
        }

    @staticmethod
    def from_dict(d):
        command = Command.from_dict(d['command'])
        return ClientRequest(
            d['client_id'], 
            d['command_id'], 
            command
        )


class ClientRequestResponse:
    def __init__(self, command_id: int, from_leader: bool, result: str=None, leader_id: str = None, reply_message: str=None, source_id: str=None):
        self.command_id = command_id
        self.from_leader = from_leader
        self.result = result
        self.leader_id = leader_id
        self.reply_message = reply_message
        self.source_id = source_id

    def to_dict(self):
        return {
            'type': 'ClientRequestResponse',
            "command_id": self.command_id,
            "from_leader": self.from_leader,
            "result": self.result,
            "leader_id": self.leader_id,
            "reply_message": self.reply_message,
            "source_id": self.source_id,
        }

    @staticmethod
    def from_dict(d):
        return ClientRequestResponse(
            command_id=d["command_id"],
            from_leader=d["from_leader"],
            result=d["result"],
            leader_id=d["leader_id"],
            reply_message=d["reply_message"],
            source_id=d["source_id"],
        )
