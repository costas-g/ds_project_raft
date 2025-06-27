import os

# Base structure for the project update
project_files = {
    "raft/raft_node.py": """\
import asyncio
import logging
import random
from raft.raft_state import RaftState
from raft.rpc.message import decode_message, encode_message
from raft.rpc.request_vote import RequestVote, RequestVoteReply
from raft.rpc.append_entries import AppendEntries, AppendEntriesReply

class RaftNode:
    def __init__(self, node_id, config, timing, log):
        self.node_id = node_id
        self.config = config
        self.timing = timing
        self.logger = log
        self.state = RaftState(node_id)
        self.peers = [nid for nid in config["nodes"] if nid != node_id]
        self.address_book = config["nodes"]
        self.current_role = "follower"
        self.votes_received = set()
        self.election_timer = None
        self.heartbeat_timer = None

    async def start(self):
        host, port = self.address_book[self.node_id].split(":")
        server = await asyncio.start_server(self.handle_connection, host, int(port))
        self.logger.info("server_started", extra={"extra": {"host": host, "port": port}})
        asyncio.create_task(self.reset_election_timer())
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        try:
            while True:
                try:
                    message = await asyncio.wait_for(decode_message(reader), timeout=2.0)
                except asyncio.TimeoutError:
                    break
                if not message:
                    break
                response = await self.handle_message(message)
                if response:
                    writer.write(encode_message(response))
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_message(self, message):
        if isinstance(message, RequestVote):
            return await self.handle_request_vote(message)
        elif isinstance(message, AppendEntries):
            return await self.handle_append_entries(message)
        return None

    async def handle_request_vote(self, msg):
        if msg.term < self.state.current_term:
            return RequestVoteReply(self.state.current_term, False)

        if msg.term > self.state.current_term:
            self.state.current_term = msg.term
            self.state.voted_for = None
            self.current_role = "follower"

        if self.state.voted_for in (None, msg.candidate_id):
            self.state.voted_for = msg.candidate_id
            return RequestVoteReply(self.state.current_term, True)

        return RequestVoteReply(self.state.current_term, False)

    async def handle_append_entries(self, msg):
        if msg.term < self.state.current_term:
            return AppendEntriesReply(self.state.current_term, False)

        self.state.current_term = msg.term
        self.current_role = "follower"
        await self.reset_election_timer()
        return AppendEntriesReply(self.state.current_term, True)

    async def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(*self.timing["election_timeout"])
        self.election_timer = asyncio.create_task(self.start_election_after(timeout))

    async def start_election_after(self, timeout):
        await asyncio.sleep(timeout)
        await self.start_election()

    async def start_election(self):
        self.current_role = "candidate"
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.logger.info("election_started", extra={"extra": {"term": self.state.current_term}})
        for peer in self.peers:
            asyncio.create_task(self.send_request_vote(peer))

        await self.reset_election_timer()

    async def send_request_vote(self, peer):
        request = RequestVote(self.state.current_term, self.node_id)
        try:
            await self.send_message(peer, request)
        except Exception as e:
            self.logger.warning("send_failed", extra={"extra": {"peer": peer, "error": str(e)}})

    async def send_message(self, peer, message):
        host, port = self.address_book[peer].split(":")
        reader, writer = await asyncio.open_connection(host, int(port))
        writer.write(encode_message(message))
        await writer.drain()
        reply = await asyncio.wait_for(decode_message(reader), timeout=2.0)
        await self.handle_reply(reply)
        writer.close()
        await writer.wait_closed()

    async def handle_reply(self, reply):
        if isinstance(reply, RequestVoteReply):
            if reply.term > self.state.current_term:
                self.state.current_term = reply.term
                self.current_role = "follower"
                self.state.voted_for = None
                return
            if reply.vote_granted:
                self.votes_received.add(reply.term)
                if len(self.votes_received) > (len(self.peers) + 1) // 2:
                    await self.become_leader()

    async def become_leader(self):
        self.current_role = "leader"
        self.logger.info("leader_elected", extra={"extra": {"term": self.state.current_term}})
        asyncio.create_task(self.send_heartbeats())

    async def send_heartbeats(self):
        while self.current_role == "leader":
            for peer in self.peers:
                msg = AppendEntries(self.state.current_term, self.node_id)
                try:
                    await self.send_message(peer, msg)
                except Exception as e:
                    self.logger.warning("heartbeat_failed", extra={"extra": {"peer": peer, "error": str(e)}})
            await asyncio.sleep(self.timing["heartbeat_interval"])
"""
}

# Write all files
for path, content in project_files.items():
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

"Updated raft_node.py written with connection timeouts and improved message handling."
