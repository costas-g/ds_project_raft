import json
import struct
from raft.rpc.request_vote import RequestVote, RequestVoteReply
from raft.rpc.append_entries import AppendEntries, AppendEntriesReply

MESSAGE_TYPES = {
    "RequestVote": RequestVote,
    "RequestVoteReply": RequestVoteReply,
    "AppendEntries": AppendEntries,
    "AppendEntriesReply": AppendEntriesReply,
}

def encode_message(msg_dict):
    body = json.dumps(msg_dict).encode('utf-8')
    length = struct.pack("!I", len(body))
    return length + body

async def read_message(reader):
    length_data = await reader.readexactly(4)
    length = struct.unpack("!I", length_data)[0]
    body = await reader.readexactly(length)
    msg_dict = json.loads(body.decode("utf-8"))
    return msg_dict
