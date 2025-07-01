import asyncio
import time
import random
from typing import Set, List, Dict
from raft.raft_state import RaftState
from raft.rpc.request_vote import RequestVote, RequestVoteReply
from raft.rpc.append_entries import AppendEntries, AppendEntriesReply
from raft.rpc.message import read_message, encode_message, MESSAGE_TYPES
from raft.entry import Entry
from raft.command import Command

class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        address_book,
        event_callback,
        heartbeat_interval,
        election_timeout_min,
        election_timeout_max,
    ):
        self.node_id = node_id
        self.peers = peers
        self.address_book = address_book

        # Persistent State
        self.state = RaftState(node_id)

        '''When referring to indexes of the log, 
        in the paper the first index is 1, hence initialized values for indexes are 0,
        but since we're implementing the log as a list in python, which is 0-based,
        we must initialize them to -1. 
        '''
        # Volatile state on all servers
        self.commit_index: int = -1   # Index of highest log entry known to be committed (none at start, increases monotonically)
        self.last_applied: int = -1   # Index of highest log entry applied to state machine (none at start, increases monotonically)

        # Volatile state on leaders (Reinitialized after election)
        self.next_index: Dict[str, int] = {}     # For each follower, index of next log entry to send (initialized later)
        self.match_index: Dict[str, int] = {}    # For each follower, highest log entry known replicated (initialized later)

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
        self.tasks: Set[asyncio.Task] = set()
        # For adding test commands to the log by the leader (no client)
        self.test_command_task = None


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
        print('shut down cleanly')

    async def ticker(self):
        # Periodic task that triggers elections or heartbeats depending on role
        while True:
            await asyncio.sleep(0.01)
            now = time.time()

            # Leader routine TODO more?
            if self.role == 'Leader':
                # Send periodic heartbeats
                if now - self.election_reset_time >= self.heartbeat_interval:
                    await self.send_heartbeats()
                    #self.election_reset_time = now

            # Follower+Candidate routine TODO more?
            else:
                # Start election after timeout
                if now - self.election_reset_time >= self.election_timeout:
                    await self.start_election()

    async def set_role(self, new_role: str):
        if self.role == 'Leader' and new_role != 'Leader':
            # Cancel test command task on stepping down
            if self.test_command_task:
                self.test_command_task.cancel()
                try:
                    await self.test_command_task
                except asyncio.CancelledError:
                    pass
                self.test_command_task = None
        self.role = new_role

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

    async def periodic_test_commands(self):
        while self.role == 'Leader':
            await asyncio.sleep(5)  # interval in seconds

            # Create a test command
            key = random.choice(['x', 'y', 'z']) # random key from these
            value = random.choice(range(0,10))  # random value from 0 to 9
            cmd_name = random.choice(Command.allowed_cmds[:3]) # random allowed command except no-op

            cmd = Command(cmd_name, key, value)

            # Wrap command in a log Entry
            entry = Entry(term=self.state.current_term, command_id=None, command=cmd)

            # Append entry to log
            self.state.log.append(entry)
            self.state.save()

            self.report(f'{self.role} {self.node_id} appended_test_command', command=repr(cmd))

            # Trigger immediate replication to peers
            await self.send_heartbeats()
            #self.election_reset_time = time.time()

    async def replicate_log_to_peer(self, peer_id):
        next_idx = self.next_index.get(peer_id, 0)
        prev_log_index = next_idx - 1
        prev_log_term = self.state.log[prev_log_index].term if prev_log_index >= 0 else 0

        # send required entries or no entries at all (heartbeat)
        entries_to_send = self.state.log[next_idx:] if self.state.get_last_log_index() >= next_idx else []

        msg = AppendEntries(
            term=self.state.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries_to_send,  # may be empty (heartbeat)
            leader_commit=self.commit_index
        ).to_dict()

        #self.report(f'{self.role} {self.node_id} SENDING {len(entries_to_send)} ENTRIES TO {peer_id}')

        task = asyncio.create_task(self.send_message(peer_id, msg))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def send_heartbeats(self):
        for peer in self.peers:
            task = asyncio.create_task(self.replicate_log_to_peer(peer))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

        # we always reset election reset time after sending heartbeats/AppendEntries RPCs
        self.election_reset_time = time.time()

        self.report(
            f'{self.role} {self.node_id} heartbeats_sent',
            term=self.state.current_term,
            next_index=dict(self.next_index),
            match_index=dict(self.match_index),
            commit_index=self.commit_index
        )

    async def handle_message(self, message: dict):
        # Handle incoming RPC messages and update node state accordingly
        msg_type = message.get('type')
        msg_term = message.get('term')

        if msg_term > self.state.current_term:
            # Update term and convert to Follower if term higher than current
            self.state.current_term = msg_term
            await self.set_role('Follower')
            self.role = 'Follower'
            self.state.voted_for = None
            self.state.save()
            self.report(f'{self.role} {self.node_id} term_updated (received {msg_type} RPC with greater term)', term=msg_term)

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
            
            reply = RequestVoteReply(
                term=self.state.current_term, 
                vote_granted=vote_granted, 
                source_id=self.node_id
            ).to_dict()

            return reply
        
        elif msg_type == 'RequestVoteReply':
            # Count votes, become Leader if majority
            if self.role == 'Candidate' and msg_term == self.state.current_term and message['vote_granted']:
                self.votes_received.add(message['source_id'])
                self.report(f'{self.role} {self.node_id} vote_received', from_node=message['source_id'], term=msg_term, total_votes=len(self.votes_received))

                if len(self.votes_received) > (len(self.peers) + 1) // 2:
                    # become Leader
                    self.role = 'Leader'

                    self.report(f'{self.role} {self.node_id} became_leader', term=self.state.current_term)

                    # Initialize nextIndex for each follower to leader’s last log index + 1
                    last_log_index = self.state.get_last_log_index()
                    self.next_index = {peer: last_log_index + 1 for peer in self.peers}
                    # Initialize matchIndex for each follower to -1 (no entries replicated yet)
                    self.match_index = {peer: -1 for peer in self.peers}

                    # Append no-op entry for leadership assertion
                    no_op_entry = Entry(self.state.current_term, command_id=None, command=Command('no-op', key=None))
                    self.state.log.append(no_op_entry)
                    self.state.save()

                    self.report(f'{self.role} {self.node_id} appended_no_op_command')

                    # Send heartbeats immediately upon becoming Leader
                    await self.send_heartbeats()
                    #self.election_reset_time = time.time()

                    # cancel the test_command_task if it already exists
                    if self.test_command_task:
                        self.test_command_task.cancel()
                        try:
                            await self.test_command_task
                        except asyncio.CancelledError:
                            pass
                    
                    # create a new period test command task
                    self.test_command_task = asyncio.create_task(self.periodic_test_commands())
                    self.tasks.add(self.test_command_task)
                    self.test_command_task.add_done_callback(self.tasks.discard)

        elif msg_type == 'AppendEntries':
            reply = self.handle_append_entries(message)
            return reply     

        elif msg_type == 'AppendEntriesReply':
            if self.role != 'Leader' or msg_term != self.state.current_term:
                return  # Ignore outdated or irrelevant replies

            peer_id = message['source_id']
            success = message['success']

            if success:
                # 1. Update nextIndex and matchIndex
                self.match_index[peer_id] = message['last_log_index']
                self.next_index[peer_id] = message['last_log_index'] + 1
                self.report(
                    f'{self.role} {self.node_id} AppendEntries successful',
                    peer=peer_id,
                    next_index=self.next_index[peer_id]
                )

                # 2. Try to advance commitIndex
                match_indexes = list(self.match_index.values()) + [self.state.get_last_log_index()]
                match_indexes.sort(reverse=True)
                majority_index = match_indexes[len(self.peers) // 2]

                if majority_index > self.commit_index:
                    entry_term = self.state.log[majority_index].term
                    if entry_term == self.state.current_term:
                        self.commit_index = majority_index
                        self.report(
                            f'{self.role} {self.node_id} commit_index_advanced',
                            new_commit_index=self.commit_index
                        )

            else:
                # AppendEntries failed (log inconsistency): decrement nextIndex and retry
                self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                self.report(
                    f'{self.role} {self.node_id} AppendEntries failed: retrying',
                    peer=peer_id,
                    next_index=self.next_index[peer_id]
                )

    def handle_append_entries(self, message: dict):
        term: int = message['term']
        leader_id = message['leader_id']
        prev_log_index: int = message['prev_log_index']
        prev_log_term: int = message['prev_log_term']
        entries_data = message['entries'] # type: Dict
        leader_commit: int = message['leader_commit']

        # helper function to streamline return points and avoid code duplication
        def reply():
            reply = AppendEntriesReply(
                term=self.state.current_term, 
                success=success, 
                last_log_index=self.state.get_last_log_index(), 
                source_id=self.node_id
            ).to_dict()
            return reply

        reason = None
        # 1. Reply false if term < currentTerm
        if term < self.state.current_term:
            success = False
            reason = 'stale term'
            self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                success=success, 
                reason = reason, 
                term=self.state.current_term, 
                log_length=len(self.state.log), 
                commit_index=self.commit_index
            )

            return reply()
        
        # Reset election timeout on valid AppendEntries
        self.election_reset_time = time.time()
        self.role = 'Follower'  # Always follower on AppendEntries from leader

        # 2. Reply false if log doesn’t contain entry at prevLogIndex with matching term
        if prev_log_index >= 0:
            if prev_log_index >= len(self.state.log):
                # Missing entry at prevLogIndex
                success = False
                reason = 'no matching entry index'
                self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                    success=success, 
                    reason = reason, 
                    term=self.state.current_term, 
                    log_length=len(self.state.log), 
                    commit_index=self.commit_index
                )

                return reply()

            if self.state.log[prev_log_index].term != prev_log_term:
                # Exists an entry at prev_log_index but terms mismatch
                success = False
                reason = 'terms mismatch'
                self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                    success=success, 
                    reason = reason, 
                    term=self.state.current_term, 
                    log_length=len(self.state.log), 
                    commit_index=self.commit_index
                )

                return reply()

        # 3. If an existing entry conflicts with a new one, delete existing entry and all that follow it

        # new_entries: List[Entry] constructed from incoming RPC
        # Entries are list of dicts, convert to Entry objects
        new_entries = [Entry.from_dict(e) for e in entries_data]

        # index at which there is a conflicting entry
        conflict_index = None

        for i, entry in enumerate(new_entries):
            index = prev_log_index + 1 + i
            if index < len(self.state.log):
                if self.state.log[index].term != entry.term:
                    # Conflict: delete entry and all that follow it
                    reason = f'conflicting entry at index {index}'
                    self.state.log = self.state.log[:index]
                    conflict_index = index
                    # After truncation, fall through to append remaining entries
                    break 
                else:
                    reason = f'entries up to index {index} already existed'
                    conflict_index = index + 1
            elif index == len(self.state.log):
                if i == 0:
                    # No conflict and log ends here - skip to append
                    reason = 'no conflicting entries'
                conflict_index = index
                break
        
        # 4. Append any new entries not already in the log
        if conflict_index is None:
            # append all entries sent by the Leader (possibly redundant since conflict_index always gets a value above)
            start = 0
        else:
            # only append the new entries, that the follower didn't already have
            start = conflict_index - (prev_log_index + 1)

        self.report(f'{self.role} {self.node_id} appending {len(new_entries[start:])} entries to my log', len_new_entries=len(new_entries), start_index=start)
        self.state.log.extend(new_entries[start:]) # mass Append happens here
        self.state.save()

        # Rule 5: Update commitIndex if leaderCommit > commitIndex
        if leader_commit > self.commit_index:
            last_new_entry_index = prev_log_index + len(new_entries)
            self.commit_index = min(leader_commit, last_new_entry_index)

        # Optionally, apply newly committed entries to state machine here or via separate async task

        success = True

        if new_entries:
            self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                success=success, 
                reason = reason, 
                term=self.state.current_term, 
                log_length=len(self.state.log), 
                commit_index=self.commit_index
            )
        else:
            self.report(f'{self.role} {self.node_id} heartbeat_received from Leader {leader_id}', commit_index=self.commit_index)
        
        return reply()

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
