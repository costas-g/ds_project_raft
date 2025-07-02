import json
import struct
from typing import Dict, Any # for linters
from asyncio import StreamReader # for linters
from raft.messages.request_vote import RequestVote, RequestVoteReply
from raft.messages.append_entries import AppendEntries, AppendEntriesReply

MESSAGE_TYPES = {
    "RequestVote": RequestVote,
    "RequestVoteReply": RequestVoteReply,
    "AppendEntries": AppendEntries,
    "AppendEntriesReply": AppendEntriesReply,
}

def encode_message(msg_dict: Dict[Any, Any]):
    body = json.dumps(msg_dict).encode('utf-8')
    length = struct.pack("!I", len(body))
    return length + body

async def read_message(reader: StreamReader):
    length_data = await reader.readexactly(4)
    length = struct.unpack("!I", length_data)[0]
    body = await reader.readexactly(length)
    msg_dict = json.loads(body.decode("utf-8"))
    return msg_dict
