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
        self.role = 'Follower'
        self.votes_received = set()
        self.election_reset_time = time.time()
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.election_timeout = self.random_timeout()
        self.event_callback = event_callback
        self.report(f'{self.role} {self.node_id} initialized_state', term=self.state.current_term, voted_for=self.state.voted_for, log_length=len(self.state.log))
        
        # Keep track of all running asyncio tasks to manage lifecycle cleanly
        self.tasks = set()

    def random_timeout(self):
        # Generate a random election timeout between min and max
        return random.uniform(self.election_timeout_min, self.election_timeout_max)

    def report(self, event, **kwargs):
        # Report an event to the callback if provided
        if self.event_callback:
            self.event_callback(self.node_id, event, kwargs)

    async def start(self):
        # Start long-running server and ticker tasks and keep references
        server_task = asyncio.create_task(self.run_server())
        ticker_task = asyncio.create_task(self.ticker())
        self.tasks.update({server_task, ticker_task})

    async def stop(self):
        # Cancel all running tasks cleanly on shutdown
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    async def ticker(self):
        # Periodic task that triggers elections or heartbeats depending on role
        while True:
            await asyncio.sleep(0.01)
            now = time.time()

            if self.role == 'Leader':
                if now - self.election_reset_time >= self.heartbeat_interval:
                    await self.send_heartbeats()
                    self.election_reset_time = now

            elif now - self.election_reset_time >= self.election_timeout:
                await self.start_election()

    async def start_election(self):
        # Start new election cycle, increment term and request votes
        self.role = 'Candidate'
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state.save()
        self.votes_received = {self.node_id}
        self.election_reset_time = time.time()
        self.election_timeout = self.random_timeout()

        self.report(f'{self.role} {self.node_id} election_started', term=self.state.current_term)

        for peer in self.peers:
            last_index = self.state.get_last_log_index()
            last_term = self.state.get_last_log_term() 
            msg = RequestVote(self.state.current_term, self.node_id, last_index, last_term).to_dict()
            task = asyncio.create_task(self.send_message(peer, msg))
            # Track task and remove from set when done
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def send_heartbeats(self):
        # Send AppendEntries heartbeats to all peers - invoked by Leader
        for peer in self.peers:
            msg = AppendEntries(self.state.current_term, self.node_id).to_dict()
            task = asyncio.create_task(self.send_message(peer, msg))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        self.report(f'{self.role} {self.node_id} heartbeats_sent')

    async def handle_message(self, message: dict):
        # Handle incoming RPC messages and update node state accordingly
        msg_type = message.get('type')
        msg_term = message.get('term')

        if msg_term > self.state.current_term:
            # Update term and convert to Follower if term higher than current
            self.state.current_term = msg_term
            self.role = 'Follower'
            self.state.voted_for = None
            self.state.save()
            self.report(f'{self.role} {self.node_id} term_updated (received RPC with greater term)', term=msg_term)

        if msg_type == 'RequestVote':
            vote_granted = False
            reason = ""

            candidate_id = message['candidate_id']
            candidate_last_index = message.get('last_log_index', -1)
            candidate_last_term = message.get('last_log_term', -1)

            # Grant vote if conditions met
            if msg_term < self.state.current_term:
                reason = f"received term {msg_term} < current term {self.state.current_term}"
            elif self.state.voted_for is None or self.state.voted_for == candidate_id:
                if self.state.is_log_up_to_date(candidate_last_index, candidate_last_term):
                    vote_granted = True
                    self.state.voted_for = candidate_id
                    self.state.save()
                    self.election_reset_time = time.time()
                    reason = "vote granted: log is up to date"
                else:
                    reason = "log not up to date"
            else:
                reason = f"already voted for {self.state.voted_for}"

            self.report(f'{self.role} {self.node_id} vote_decision', candidate_id=candidate_id, vote_granted=vote_granted, reason=reason)

            reply = {
                'type': 'RequestVoteReply',
                'term': self.state.current_term,
                'vote_granted': vote_granted,
                'source': self.node_id
            }
            return reply

        elif msg_type == 'RequestVoteReply':
            # Count votes, become Leader if majority
            if self.role == 'Candidate' and msg_term == self.state.current_term and message['vote_granted']:
                self.votes_received.add(message['source'])
                self.report(f'{self.role} {self.node_id} vote_received', from_node=message['source'], term=msg_term, total_votes=len(self.votes_received))
                if len(self.votes_received) > (len(self.peers) + 1) // 2:
                    self.role = 'Leader'
                    self.election_reset_time = time.time()
                    self.report(f'{self.role} {self.node_id} became_leader', term=self.state.current_term)

        elif msg_type == 'AppendEntries':
            # Reset election timer on heartbeat from Leader
            if msg_term == self.state.current_term:
                self.role = 'Follower'
                self.election_reset_time = time.time()
                self.report(f'{self.role} {self.node_id} heartbeat_received from Leader {message['leader_id']}') #, leader_id=message['leader_id'])

    async def send_message(self, peer_id, message):
        # Send a message over TCP to a peer and handle response
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
            self.report(f'{self.role} {self.node_id} send_failed to peer {peer_id}', error=str(e)) # , peer=peer_id, error=str(e))

    async def run_server(self):
        # TCP server that listens for incoming messages from peers
        host, port = self.address_book[self.node_id].split(':')
        server = await asyncio.start_server(self.handle_connection, host, int(port))
        async with server:
            self.report(f'{self.role} {self.node_id} server_started', host=host, port=port)
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        # Handle an incoming connection, read and respond to messages
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
