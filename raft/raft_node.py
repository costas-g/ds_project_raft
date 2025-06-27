import asyncio
import time
import random
from raft.raft_state import RaftState
from raft.rpc.request_vote import RequestVote
from raft.rpc.append_entries import AppendEntries
from raft.rpc.message import read_message, encode_message

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
