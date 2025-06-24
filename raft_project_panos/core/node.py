import threading
import time
import random
from enum import Enum
from queue import Queue

from core.messages import RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse
from core.state import RaftState
from core.log import RaftLog
from core.state_machine import StateMachine
from config import NODE_IDS, ELECTION_TIMEOUT_RANGE, HEARTBEAT_INTERVAL
from network.transport import send_message, register_node
from performance.batching import BatchingManager
from performance.snapshot import SnapshotManager
from performance.executor import ParallelExecutor

class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class Node(threading.Thread):
    def __init__(self, node_id, inbox: Queue):
        super().__init__()
        self.node_id = node_id
        self.role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for = None

        self.inbox = inbox
        self.election_timeout = self._reset_timeout()
        self.last_heartbeat = time.time()
        self.running = True

        self.log = RaftLog()
        self.state_machine = StateMachine()
        self.executor = ParallelExecutor(self.state_machine)
        self.snapshot_manager = SnapshotManager(self.node_id, self.state_machine, self.log)
        self.snapshot_manager.load_snapshot()

        self.batching = None
        self.vote_count = 0

        register_node(self.node_id, self.inbox)

    def _reset_timeout(self):
        return random.uniform(*ELECTION_TIMEOUT_RANGE)

    def run(self):
        while self.running:
            now = time.time()

            if self.role == Role.FOLLOWER:
                if now - self.last_heartbeat > self.election_timeout:
                    print(f"[{self.node_id}] Election timeout. Becoming Candidate.")
                    self.start_election()

            elif self.role == Role.CANDIDATE:
                if self.vote_count > len(NODE_IDS) // 2:
                    self.become_leader()

            elif self.role == Role.LEADER:
                if now - self.last_heartbeat >= HEARTBEAT_INTERVAL:
                    self.send_heartbeats()
                    self.last_heartbeat = now

            self.process_inbox()
            time.sleep(0.1)

    def start_election(self):
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.vote_count = 1  # Ψηφίζει τον εαυτό του
        self.last_heartbeat = time.time()
        self.election_timeout = self._reset_timeout()

        print(f"[{self.node_id}] Starting election for term {self.current_term}")

        for peer_id in NODE_IDS:
            if peer_id != self.node_id:
                msg = RequestVote(term=self.current_term, candidate_id=self.node_id)
                send_message(peer_id, msg)

    def become_leader(self):
        self.role = Role.LEADER
        self.last_heartbeat = time.time()
        print(f"[{self.node_id}] Became leader for term {self.current_term}")

        self.batching = BatchingManager(self.node_id, self.current_term)
        self.batching.start()
        self.executor.start()

    def send_heartbeats(self):
        for peer_id in NODE_IDS:
            if peer_id != self.node_id:
                msg = AppendEntries(term=self.current_term, leader_id=self.node_id)
                send_message(peer_id, msg)

    def process_inbox(self):
        while not self.inbox.empty():
            msg = self.inbox.get()

            if isinstance(msg, AppendEntries):
                self.handle_append_entries(msg)

            elif isinstance(msg, RequestVote):
                self.handle_request_vote(msg)

            elif isinstance(msg, RequestVoteResponse):
                self.handle_vote_response(msg)

    def handle_append_entries(self, msg):
        if msg.term >= self.current_term:
            self.role = Role.FOLLOWER
            self.current_term = msg.term
            self.last_heartbeat = time.time()
            print(f"[{self.node_id}] Received heartbeat from leader {msg.sender_id}")

            for command in msg.entries:
                self.executor.submit(command)
        else:
            print(f"[{self.node_id}] Ignored outdated AppendEntries from term {msg.term}")

    def handle_request_vote(self, msg):
        if msg.term > self.current_term:
            self.current_term = msg.term
            self.voted_for = msg.candidate_id
            print(f"[{self.node_id}] Voted for {msg.candidate_id} in term {msg.term}")
            response = RequestVoteResponse(term=self.current_term, sender_id=self.node_id, vote_granted=True)
            send_message(msg.candidate_id, response)
        else:
            print(f"[{self.node_id}] Rejected vote for {msg.candidate_id} in term {msg.term}")
            response = RequestVoteResponse(term=self.current_term, sender_id=self.node_id, vote_granted=False)
            send_message(msg.candidate_id, response)

    def handle_vote_response(self, msg):
        if self.role == Role.CANDIDATE and msg.term == self.current_term:
            if msg.vote_granted:
                self.vote_count += 1
                print(f"[{self.node_id}] Received vote from {msg.sender_id} (total: {self.vote_count})")

    def stop(self):
        self.running = False
        if self.batching:
            self.batching.stop()
        self.executor.stop()
        self.snapshot_manager.create_snapshot()
