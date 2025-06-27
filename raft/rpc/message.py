import json
import struct

def encode_message(msg_dict):
    data = json.dumps(msg_dict).encode()
    return struct.pack('>I', len(data)) + data

async def read_message(reader):
    length_bytes = await reader.readexactly(4)
    length = struct.unpack('>I', length_bytes)[0]
    body = await reader.readexactly(length)
    return json.loads(body.decode())
