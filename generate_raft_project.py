import os
print('Hello world')
files = {
    "raft/__init__.py": "",
    "raft/raft_state.py": '''import json
import os

class RaftState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.state_file = f'state_{node_id}.json'
        self.load()

    def load(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                self.current_term = data.get('current_term', 0)
                self.voted_for = data.get('voted_for', None)
                self.log = data.get('log', [])

    def save(self):
        data = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
        }
        with open(self.state_file, 'w') as f:
            json.dump(data, f)
''',
    "raft/rpc/__init__.py": "",
    "raft/rpc/request_vote.py": '''class RequestVote:
    def __init__(self, term, candidate_id):
        self.term = term
        self.candidate_id = candidate_id

    def to_dict(self):
        return {
            'type': 'RequestVote',
            'term': self.term,
            'candidate_id': self.candidate_id
        }

    @staticmethod
    def from_dict(d):
        return RequestVote(d['term'], d['candidate_id'])
''',
    "raft/rpc/append_entries.py": '''class AppendEntries:
    def __init__(self, term, leader_id):
        self.term = term
        self.leader_id = leader_id

    def to_dict(self):
        return {
            'type': 'AppendEntries',
            'term': self.term,
            'leader_id': self.leader_id
        }

    @staticmethod
    def from_dict(d):
        return AppendEntries(d['term'], d['leader_id'])
''',
    "raft/rpc/message.py": '''import json
import struct

def encode_message(msg_dict):
    data = json.dumps(msg_dict).encode()
    return struct.pack('>I', len(data)) + data

async def read_message(reader):
    length_bytes = await reader.readexactly(4)
    length = struct.unpack('>I', length_bytes)[0]
    body = await reader.readexactly(length)
    return json.loads(body.decode())
''',
    "raft/raft_node.py": '''import asyncio
import time
import random
from raft.raft_state import RaftState
from raft.rpc.request_vote import RequestVote
from raft.rpc.append_entries import AppendEntries

class RaftNode:
    def __init__(
        self,
        node_id,
        peers,
        address_book,
        event_callback,
        heartbeat_interval,
        election_timeout_min,
        election_timeout_max,
    ):
        self.node_id = node_id
        self.peers = peers
        self.address_book = address_book
        self.state = RaftState(node_id)
        self.role = 'follower'
        self.votes_received = set()
        self.election_reset_time = time.time()
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.election_timeout = self.random_timeout()
        self.event_callback = event_callback

    def random_timeout(self):
        return random.uniform(self.election_timeout_min, self.election_timeout_max)

    def report(self, event, **kwargs):
        if self.event_callback:
            self.event_callback(self.node_id, event, kwargs)

    async def start(self):
        asyncio.create_task(self.run_server())
        asyncio.create_task(self.ticker())

    async def ticker(self):
        while True:
            await asyncio.sleep(0.01)
            now = time.time()

            if self.role == 'leader':
                if now - self.election_reset_time >= self.heartbeat_interval:
                    await self.send_heartbeats()
                    self.election_reset_time = now

            elif now - self.election_reset_time >= self.election_timeout:
                await self.start_election()

    async def start_election(self):
        self.role = 'candidate'
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state.save()
        self.votes_received = {self.node_id}
        self.election_reset_time = time.time()
        self.election_timeout = self.random_timeout()

        self.report('election_started', term=self.state.current_term)

        for peer in self.peers:
            msg = RequestVote(self.state.current_term, self.node_id).to_dict()
            asyncio.create_task(self.send_message(peer, msg))

    async def send_heartbeats(self):
        for peer in self.peers:
            msg = AppendEntries(self.state.current_term, self.node_id).to_dict()
            asyncio.create_task(self.send_message(peer, msg))
        self.report('heartbeats_sent')

    async def handle_message(self, message: dict):
        msg_type = message.get('type')
        term = message.get('term')

        if term > self.state.current_term:
            self.state.current_term = term
            self.role = 'follower'
            self.state.voted_for = None
            self.state.save()
            self.report('term_updated', term=term)

        if msg_type == 'RequestVote':
            vote_granted = False
            if term == self.state.current_term and (self.state.voted_for is None or self.state.voted_for == message['candidate_id']):
                vote_granted = True
                self.state.voted_for = message['candidate_id']
                self.state.save()
                self.election_reset_time = time.time()

            reply = {
                'type': 'RequestVoteReply',
                'term': self.state.current_term,
                'vote_granted': vote_granted,
                'source': self.node_id
            }
            return reply

        elif msg_type == 'RequestVoteReply':
            if self.role == 'candidate' and term == self.state.current_term and message['vote_granted']:
                self.votes_received.add(message['source'])
                if len(self.votes_received) > (len(self.peers) + 1) // 2:
                    self.role = 'leader'
                    self.election_reset_time = time.time()
                    self.report('became_leader', term=self.state.current_term)

        elif msg_type == 'AppendEntries':
            if term == self.state.current_term:
                self.role = 'follower'
                self.election_reset_time = time.time()
                self.report('heartbeat_received', leader_id=message['leader_id'])

    async def send_message(self, peer_id, message):
        import asyncio
        from raft.rpc.message import encode_message, read_message
        try:
            host, port = self.address_book[peer_id].split(':')
            reader, writer = await asyncio.open_connection(host, int(port))
            writer.write(encode_message(message))
            await writer.drain()

            response = await read_message(reader)
            await self.handle_message(response)

            writer.close()
            await writer.wait_closed()
        except Exception as e:
            self.report('send_failed', peer=peer_id, error=str(e))

    async def run_server(self):
        import asyncio
        from raft.rpc.message import read_message, encode_message

        host, port = self.address_book[self.node_id].split(':')
        server = await asyncio.start_server(self.handle_connection, host, int(port))
        async with server:
            self.report('server_started', host=host, port=port)
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        from raft.rpc.message import encode_message
        try:
            while True:
                message = await read_message(reader)
                response = await self.handle_message(message)
                if response:
                    writer.write(encode_message(response))
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
''',
    "run/__init__.py": "",
    "run/cluster_config.json": '''{
  "nodes": ["n1", "n2", "n3"],
  "addresses": {
    "n1": "127.0.0.1:5001",
    "n2": "127.0.0.1:5002",
    "n3": "127.0.0.1:5003"
  },
  "timing": {
    "heartbeat_interval": 0.05,
    "election_timeout_min": 0.15,
    "election_timeout_max": 0.3
  }
}
''',
    "run/run_node.py": '''import asyncio
import json
import os
import logging
from raft.raft_node import RaftNode

def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'cluster_config.json')) as f:
        config = json.load(f)

    timing = config.get("timing")
    if timing is None:
        raise RuntimeError("Missing 'timing' section in config")

    required_keys = ["heartbeat_interval", "election_timeout_min", "election_timeout_max"]
    for key in required_keys:
        if key not in timing:
            raise RuntimeError(f"Missing timing parameter: {key}")

    return config

def setup_logger(node_id):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/{node_id}.log")
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def event_logger(node_id, event, data):
    logger = logging.getLogger(node_id)
    msg = f"{event}: {data}"
    logger.info(msg)

async def main(node_id):
    config = load_config()
    peers = [n for n in config["nodes"] if n != node_id]
    timing = config["timing"]

    node = RaftNode(
        node_id,
        peers,
        config["addresses"],
        event_callback=event_logger,
        heartbeat_interval=timing["heartbeat_interval"],
        election_timeout_min=timing["election_timeout_min"],
        election_timeout_max=timing["election_timeout_max"],
    )
    
    logger = setup_logger(node_id)
    logger.info(f"Node {node_id} starting")

    await node.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run_node.py <node_id>")
        exit(1)
    node_id = sys.argv[1]
    asyncio.run(main(node_id))
''',
}
print('hello world2')
for filepath, content in files.items():
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

print("All files created successfully.")
