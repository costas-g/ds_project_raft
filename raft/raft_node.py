import asyncio
import time
import random
from typing import Set, List, Dict
from raft.raft_state import RaftState
from raft.messages.request_vote import RequestVote, RequestVoteReply
from raft.messages.append_entries import AppendEntries, AppendEntriesReply
from raft.messages.client_request import ClientRequest, ClientRequestResponse
from raft.messages.install_snapshot import InstallSnapshot, InstallSnapshotReply
from raft.messages.message import read_message, encode_message, MESSAGE_TYPES
from raft.entry import Entry
from raft.command import Command
from raft.state_machine import StateMachine
from raft.snapshot import Snapshot
from run.cluster_config import print_out, CLOCK_PERIOD, MAX_LOG, lan_ip_addresses

class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        address_book,
        # client_port,
        event_callback,
        batching_interval,
        heartbeat_interval,
        election_timeout_min,
        election_timeout_max,
    ):
        # Basic node attributes
        self.node_id = node_id
        self.peers = peers
        self.address_book = address_book
        self.leader_id: str = None
        self.role = 'Follower'
        self.votes_received = set()

        # Persistent State initialize and load from state files.
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

        # Client related attributes
        # self.client_port = client_port
        self.pending_client_responses: Dict[int, asyncio.Future] = {}

        # State Machine initialization
        self.state_machine = StateMachine()
         # Load snapshot data (if it exists) to state machine
        if self.state.snapshot.data:
            self.state_machine.load_snapshot(self.state.snapshot.data)

        # Properly initialize commit_index and last_applied given snapshot
        self.commit_index: int = self.state.snapshot.last_included_index   # Index of highest log entry known to be committed (increases monotonically)
        self.last_applied: int = self.state.snapshot.last_included_index   # Index of highest log entry applied to state machine (increases monotonically)

        # Snapshot related attributes
        self.snapshot_threshold: int = MAX_LOG  # create snapshot every X applied entries
        
        # Lease for read commands
        self.lease_ack_set = set()  # reset every heartbeat round
        self.last_lease_confirmed_time = None

        # Timing attributes
        self.election_reset_time = time.monotonic()
        self.heartbeat_reset_time = time.monotonic()
        self.batching_interval = batching_interval
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.election_timeout = self.random_timeout()
        self.event_callback = event_callback

        self.report(f'{self.role} {self.node_id} initialized_state', term=self.state.current_term, voted_for=self.state.voted_for, last_snapshot_index=self.state.snapshot.last_included_index, log_length=len(self.state.log))
        
        # Keep track of all running asyncio tasks to manage lifecycle cleanly
        self.tasks: Set[asyncio.Task] = set()
        # # For adding test commands to the log by the leader (no client)
        # self.test_command_task = None


    def random_timeout(self):
        '''Generate a random election timeout between min and max'''
        return random.uniform(self.election_timeout_min, self.election_timeout_max)

    def report(self, event, **kwargs):
        '''Report an event to the callback if provided'''
        if self.event_callback:
            self.event_callback(self.node_id, event, kwargs)

    async def start(self):
        '''Start long-running server and ticker tasks and keep references'''
        server_task = asyncio.create_task(self.run_server())
        ticker_task = asyncio.create_task(self.ticker())
        self.tasks.update({server_task, ticker_task})

        # Start long-running client server task 
        client_server_task = asyncio.create_task(self.start_client_server())
        self.tasks.add(client_server_task)

        print('Server started')

    async def stop(self):
        '''Cancel all running tasks cleanly on shutdown'''
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        print('Shut down cleanly')

    async def ticker(self):
        '''Periodic task that triggers elections or heartbeats depending on role'''
        prev_last_log_index = self.state.get_last_log_index() # initialization
        while True:
            await asyncio.sleep(CLOCK_PERIOD)
            now = time.monotonic()

            # Leader routine 
            if self.role == 'Leader':
                # Send batches of entries if last_log_index has been advanced (i.e. if new commands from clients)
                # This needs to be checked before the empty heartbeats because they have priority over them
                if now - self.heartbeat_reset_time >= self.batching_interval:
                    if self.state.get_last_log_index() > prev_last_log_index:
                        await self.send_heartbeats()

                    prev_last_log_index = self.state.get_last_log_index()

                # Send periodic heartbeats
                if now - self.heartbeat_reset_time >= self.heartbeat_interval:
                    await self.send_heartbeats() # heartbeat timer gets reset in the send_heartbeats method

            # Follower+Candidate routine 
            else:
                # Start election after timeout
                if now - self.election_reset_time >= self.election_timeout:
                    await self.start_election()

            # # Apply commited entries routine for all
            # if self.last_applied < self.commit_index:
            #     self.apply_committed_entries()

    async def set_role(self, new_role: str):
        if self.role == 'Leader' and new_role != 'Leader':
            pass
            # # Cancel test command task on stepping down
            # if self.test_command_task:
            #     self.test_command_task.cancel()
            #     try:
            #         await self.test_command_task
            #     except asyncio.CancelledError:
            #         pass
            #     self.test_command_task = None
        self.role = new_role

    async def start_election(self):
        # Start new election cycle, increment term and request votes
        self.role = 'Candidate'
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state.save()
        self.votes_received = {self.node_id}
        self.reset_election_timer()

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
            cmd_name = random.choice(Command.allowed_cmds[1:]+['qwerty', 'asdqwe']) # random commands except no-op

            cmd = Command(cmd_name, key, value)

            # Wrap command in a log Entry
            entry = Entry(term=self.state.current_term, command_id=None, command=cmd)

            # Append entry to log
            self.state.log.append(entry)
            self.state.save()
            self.state.append_log_entry_to_file(entry)

            self.report(f'{self.role} {self.node_id} appended_test_command', command=repr(cmd))

            # Trigger immediate replication to peers
            await self.send_heartbeats()

    async def handle_client_message(self, message):#, writer):
        try:
            request = ClientRequest.from_dict(message)
            print_out and print(f'Received command from {request.client_id}: {' '.join(str(v) for v in request.command.to_dict().values())}')
            self.report(f'{self.role} {self.node_id} received command from client {request.client_id}: {' '.join(str(v) for v in request.command.to_dict().values())}')#{request.command.to_dict()}')
        except Exception as e:
            response = ClientRequestResponse(
                command_id=None,
                from_leader=True,  # We're replying, so we're the leader (even if not real)
                result=None,
                leader_id = self.leader_id,
                reply_message="Invalid request format",
                source_id=self.node_id
            ).to_dict()
            return response

        if self.role != "Leader":
            # Redirect client to known leader (if any)
            response = ClientRequestResponse(
                command_id=request.command_id,
                from_leader=False,
                result=None,
                leader_id=self.leader_id,
                reply_message=f"Not leader. Try contacting {self.leader_id}",
                source_id=self.node_id
            ).to_dict()
            return response

        # We are the leader 
        # 
        # Check if request command is INVALID and handle it
        if request.command.cmd_type.lower() not in Command.allowed_cmds[1:]: # if not in all allowed commands except no-op
            response = ClientRequestResponse(
                command_id=request.command_id,
                from_leader=True,
                result=f'Unknown command: {request.command.cmd_type}',
                leader_id=self.leader_id,
                reply_message=f"List of known commands: {Command.allowed_cmds[1:]}",
                source_id=self.node_id
            ).to_dict()
            return response

        # Check if request command is READ 
        # handle it without appending to log
        if request.command.cmd_type.lower() == 'read': 
            # Check if time elapsed since last heartbeats received from majority is old
            # if old, send heartbeats to confirm leader is recent after heartbeat ACKs from majority
            # then reply to client read request
            now = time.monotonic()
            if self.last_lease_confirmed_time is not None and \
                (now - self.heartbeat_reset_time) > self.batching_interval and \
                (now - self.last_lease_confirmed_time) > self.election_timeout_min / 2: # < lease_duration = election_timeout_min - (max_clock_drift + max_network_delay):
                # Lease expired - trigger fresh heartbeat round to reestablish leadership
                await self.send_heartbeats()
                print('Sent heartbeats for lease update')

                if self.last_lease_confirmed_time is not None and \
                (now - self.last_lease_confirmed_time) > self.election_timeout_min / 2:
                    response = ClientRequestResponse(
                        command_id=request.command_id,
                        from_leader=True,
                        result='Failed to read',
                        leader_id=self.leader_id,
                        reply_message='Stale leader or lease not confirmed',
                        source_id=self.node_id
                    ).to_dict()
                    return response

            # Safe to serve read now
            key = request.command.key
            read_value = self.state_machine.get(key)
            if read_value is not None:
                result = f"Read {key} = {read_value}"
                reply_message = 'OK. No append to log'
            else:
                result = f"{key} not found for read"
                reply_message = 'OK. Try a different key'
            
            response = ClientRequestResponse(
                command_id=request.command_id,
                from_leader=True,
                result=result,
                leader_id=self.leader_id,
                reply_message=reply_message,
                source_id=self.node_id
            ).to_dict()
            return response

        # Request command is valid and not READ, append command to log
        entry = Entry(
            term=self.state.current_term,
            command_id=request.command_id,
            command=request.command
        )
        self.state.log.append(entry)
        # self.report(f'COMMAND ADDED TO LOG NOW')
        self.state.save()
        self.state.append_log_entry_to_file(entry)

        # send heartbeats immediately upon receiving client command - or wait for batching them
        # await self.send_heartbeats()

        log_index = self.state.get_last_log_index()

        # Register a future to wait for commit
        future = asyncio.Future()
        self.pending_client_responses[log_index] = future

        # Wait for commit or timeout
        try:
            result = await asyncio.wait_for(future, timeout=10.0)
            reply_message = "OK. Command committed"
        except asyncio.TimeoutError:
            self.pending_client_responses.pop(log_index, None)
            result = "Request timed out from server".upper()
            reply_message = "OK. Request timed out before commit"

        response = ClientRequestResponse(
            command_id=request.command_id,
            from_leader=True,
            result=result,
            leader_id=self.leader_id,
            reply_message=reply_message,
            source_id=self.node_id
        ).to_dict()
        return response

    def apply_committed_entries(self):
        start_index = None # for report logging 
        while self.last_applied < self.commit_index:
            next_to_apply_idx = self.last_applied + 1

            cmd = self.state.log[self.state.global_to_local(next_to_apply_idx)].command
            # apply command, save result from return
            result = self.state_machine.apply(cmd)
            self.last_applied += 1
            
            # Notify client if pending
            fut = self.pending_client_responses.pop(self.last_applied, None)
            if fut and not fut.done():
                fut.set_result(result)

            if start_index is None:
                start_index = self.last_applied

            # Code for applying commutative commands concurrently
            # We would actually need multiprocessing in Python for real parallelism due to the GIL limit
            # But for simple CRUD commands where the CPU work is negligible, there would be no real gain  
            # first_log_index_in_batch = next_to_apply_idx
            # batch = []
            # used_keys = set()

            # # Look ahead and build a batch of non-conflicting commands
            # j = next_to_apply_idx
            # while j <= self.commit_index:
            #     cmd = self.state.log[self.state.global_to_local(j)].command
            #     key = cmd.key
            #     if key in used_keys:
            #         break
            #     used_keys.add(key)
            #     batch.append(cmd)
            #     j += 1

            # # Apply the batch concurrently
            # tasks = [asyncio.create_task(self.state_machine.apply(c)) for c in batch]
            # results = await asyncio.gather(*tasks)

            # self.last_applied = j - 1
            # next_to_apply_idx = j
            # if start_index is None:
            #     start_index = first_log_index_in_batch

            # # Notify client if pending
            # for i, result in enumerate(results, start=first_log_index_in_batch):
            #     fut = self.pending_client_responses.pop(i, None)
            #     if fut and not fut.done():
            #         fut.set_result(result)

        # For reporting to the log and to the console
        if start_index is not None:
            end_index = self.last_applied
            count = end_index - start_index + 1
            entries_str = 'entry'
            if count > 1:
                entries_str = 'entries'
            print_out and print(f'Applied {count} {entries_str}. SM:', self.state_machine.dump())
            self.report(f'{self.role} {self.node_id} applied_batch of {count} {entries_str} to the SM')#, start=start_index, end=end_index, count=count)

        # Create Snapshot if max threshold is exceeded
        if self.last_applied - self.state.snapshot.last_included_index >= self.snapshot_threshold:
            self.create_snapshot()
            print_out and print(f'Created own snapshot')
            self.report(f'{self.role} {self.node_id} snapshot_created', snapshot_last_index=self.state.snapshot.last_included_index, snapshot_last_term=self.state.snapshot.last_included_term, log_length=len(self.state.log))
            
    async def replicate_log_to_peer(self, peer_id):
        next_idx = self.next_index.get(peer_id, 0)
        prev_log_index = next_idx - 1

        if next_idx > self.state.snapshot.last_included_index:
            # Send AppendEntries RPC
            # prev_log_term can only be calculated now when the respective entry hasn't been discarded
            prev_log_term = self.state.get_term_at_index(prev_log_index) # self.state.log[self.state.global_to_local(prev_log_index)].term if prev_log_index >= 0 else 0
            # send required entries or no entries at all (heartbeat)
            entries_to_send = self.state.log[self.state.global_to_local(next_idx):] if self.state.get_last_log_index() >= next_idx else []

            msg = AppendEntries(
                term=self.state.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_to_send,  # may be empty (heartbeat)
                leader_commit=self.commit_index
            ).to_dict()

            #self.report(f'{self.role} {self.node_id} SENDING {len(entries_to_send)} ENTRIES TO {peer_id}')
        else:
            # Send InstallSnapshot RPC first
            msg = InstallSnapshot(
                term=self.state.current_term,
                leader_id=self.node_id,
                last_included_index=self.state.snapshot.last_included_index,
                last_included_term=self.state.snapshot.last_included_term,
                snapshot_data=self.state.snapshot.data
            ).to_dict()
            print(f'Sent snapshot to {peer_id}')
            self.report(f'{self.role} {self.node_id} snapshot_sent to {peer_id}', last_included_index=self.state.snapshot.last_included_index)
            # Update peer's next_index for the next heartbeat 
            self.next_index[peer_id] = self.state.snapshot.last_included_index + 1

        task = asyncio.create_task(self.send_message(peer_id, msg))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def send_heartbeats(self):
        for peer in self.peers:
            task = asyncio.create_task(self.replicate_log_to_peer(peer))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

        # Reset the heartbeat timer
        self.heartbeat_reset_time = time.monotonic()
        # Reset the lease ack set after leader sends RPCs
        self.lease_ack_set.clear()
        # self.report(f'TIME RESET')

        self.report('-------------------------')
        self.report(
            f'{self.role} {self.node_id} heartbeats_sent',
            term=self.state.current_term,
            next_index=dict(self.next_index),
            match_index=dict(self.match_index),
            commit_index=self.commit_index
        )

    def reset_election_timer(self):
        '''Reset the election timer after follower receives valid  RPCs. Also randomize the next election timeout period.'''
        self.election_reset_time = time.monotonic()
        self.election_timeout = self.random_timeout()

    async def handle_message(self, message: dict):
        # Handle incoming RPC messages and update node state accordingly
        msg_type = message.get('type')
        msg_term = message.get('term')

        if msg_term > self.state.current_term:
            # Update term and convert to Follower if term higher than current
            self.state.current_term = msg_term
            await self.set_role('Follower')
            self.reset_election_timer()
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
                    self.reset_election_timer()
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
                    self.leader_id = self.node_id
                    print('Became Leader')
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
                    self.state.append_log_entry_to_file(no_op_entry)

                    self.report(f'{self.role} {self.node_id} appended_no_op_command')

                    # Send heartbeats immediately upon becoming Leader
                    await self.send_heartbeats()

                    # # cancel the test_command_task if it already exists
                    # if self.test_command_task:
                    #     self.test_command_task.cancel()
                    #     try:
                    #         await self.test_command_task
                    #     except asyncio.CancelledError:
                    #         pass
                    
                    # # create a new period test command task
                    # self.test_command_task = asyncio.create_task(self.periodic_test_commands())
                    # self.tasks.add(self.test_command_task)
                    # self.test_command_task.add_done_callback(self.tasks.discard)

        elif msg_type == 'AppendEntries':
            reply = self.handle_append_entries(message)
            return reply     

        elif msg_type == 'AppendEntriesReply':
            if self.role != 'Leader' or msg_term != self.state.current_term:
                return  # Ignore outdated or irrelevant replies

            peer_id = message['source_id']
            success = message['success']
            conflict_term = message['conflict_term']
            conflict_term_first_index = message['conflict_term_first_index']
            peer_last_log_index = message['last_log_index']

            self.refresh_leader_lease_on_valid_reply_from(peer_id)

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
                    entry_term = self.state.get_term_at_index(majority_index) # self.state.log[self.state.global_to_local(majority_index)].term
                    if entry_term == self.state.current_term:
                        self.commit_index = majority_index
                        self.report(f'{self.role} {self.node_id} commit_index_advanced', new_commit_index=self.commit_index)
                        # Apply commited entries 
                        if self.last_applied < self.commit_index:
                            self.apply_committed_entries()

            else:
                # AppendEntries failed (log inconsistency): decrement nextIndex and retry
                # self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                if conflict_term_first_index is not None:
                    self.next_index[peer_id] = max(0, conflict_term_first_index)
                else:
                    if self.next_index[peer_id] > peer_last_log_index + 1:
                        self.next_index[peer_id] = max(0, peer_last_log_index + 1)
                    else:
                        self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)

                self.report(
                    f'{self.role} {self.node_id} AppendEntries failed: retrying',
                    peer=peer_id,
                    next_index=self.next_index[peer_id]
                )

        elif msg_type == 'InstallSnapshot':
            # Step 1: Reply immediately if term < currentTerm
            if msg_term < self.state.current_term:
                success = False
                reason = 'stale term'

                self.report(f'{self.role} {self.node_id} InstallSnapshot_processed', success=success, reason = reason, term=self.state.current_term)

                reply = InstallSnapshotReply(
                    term=self.state.current_term,
                    success=success,
                    source_id=self.node_id
                ).to_dict()
                return reply
            
            # Reset election timeout upon receving valid RPCs
            self.reset_election_timer()
            self.role = 'Follower'  # Always follower on RPCs from leader
            # update leader_id to redirect clients
            self.leader_id = message.get('leader_id', None)

            # Steps 2, 3, 5, 6, 7: Create, write, and save new snapshot, truncate log as needed
            last_included_index = message.get('last_included_index', None)

            if last_included_index <= self.state.snapshot.last_included_index:
                # Snapshot is older or same - ignore it
                success = False
                reason = 'older or same snapshot'

                self.report(f'{self.role} {self.node_id} InstallSnapshot_processed', success=success, reason = reason, snapshot_index=last_included_index)

                reply = InstallSnapshotReply(
                    term=self.state.current_term,
                    success=success,
                    source_id=self.node_id
                ).to_dict()
                return reply
            
            # Snapshot is newer - save it
            last_included_term = message.get('last_included_term', None)
            snapshot_data = message.get('snapshot_data', None)

            self.state.install_snapshot(last_included_index, last_included_term, snapshot_data)

            # Step 8: reset state machine to snapshot state
            self.state_machine.load_snapshot(snapshot_data)
            
            print(f'Installed received snapshot')
            self.report(f'{self.role} {self.node_id} InstallSnapshot_processed', success=True, reason = 'newer snapshot', snapshot_index=last_included_index)

            # Update commit and last applied indexes to snapshot index
            self.commit_index = last_included_index
            self.last_applied = last_included_index

            # Apply commited entries 
            if self.last_applied < self.commit_index:
                self.apply_committed_entries()

            reply = InstallSnapshotReply(
                term=self.state.current_term,
                success=True,
                source_id=self.node_id
            ).to_dict()
            return reply

        elif msg_type == 'InstallSnapshotReply':
            if self.role != 'Leader' or msg_term != self.state.current_term:
                return  # Ignore outdated or irrelevant replies

            peer_id = message['source_id']
            success = message['success']

            self.refresh_leader_lease_on_valid_reply_from(peer_id)

            # No need to do anything after the reply
            self.report(
                f'{self.role} {self.node_id} InstallSnapshotReply received',
                peer=peer_id,
                success=success
            )
            return

    def refresh_leader_lease_on_valid_reply_from(self, peer_id):
        self.lease_ack_set.add(peer_id)
        if len(self.lease_ack_set) >= len(self.peers) / 2:
            self.last_lease_confirmed_time = time.monotonic()
            self.report(f'LEASE REFRESHED')

    def handle_append_entries(self, message: dict):
        term: int = message['term']
        leader_id = message['leader_id']
        prev_log_index: int = message['prev_log_index']
        prev_log_term: int = message['prev_log_term']
        entries_data = message['entries'] # type: Dict
        leader_commit: int = message['leader_commit']

        conflict_term = None
        conflict_term_first_index = None

        if entries_data:
            self.report(f'{self.role} {self.node_id} AppendEntries_RPC_received from Leader {leader_id}', term=term, prev_log_index=prev_log_index)
        else:
            self.report(f'{self.role} {self.node_id} heartbeat_received from Leader {leader_id}', term=term)

        # helper function to streamline return points and avoid code duplication
        def reply():
            reply = AppendEntriesReply(
                term=self.state.current_term, 
                success=success, 
                last_log_index=self.state.get_last_log_index(), 
                source_id=self.node_id,
                conflict_term=conflict_term,
                conflict_term_first_index=conflict_term_first_index
            ).to_dict()
            return reply

        reason = None
        # 1. Reply false if term < currentTerm
        if term < self.state.current_term:
            success = False
            reason = 'stale term'

            conflict_term = None
            conflict_term_first_index = None

            self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                success=success, 
                reason = reason, 
                term=self.state.current_term, 
                last_log_index=self.state.get_last_log_index(), 
                commit_index=self.commit_index
            )
            return reply()
        
        # Reset election timeout on receiving valid RPCs
        self.reset_election_timer()
        self.role = 'Follower'  # Always follower on AppendEntries from leader
        # update leader_id to redirect clients
        self.leader_id = leader_id

        # 2. Reply false if log doesn’t contain entry at prevLogIndex with matching term
        if prev_log_index >= 0:
            if prev_log_index >= self.state.get_last_log_index() + 1:
                # Missing entry at prevLogIndex
                success = False
                reason = 'no matching entry index'

                conflict_term = None
                conflict_term_first_index = None

                self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                    success=success, 
                    reason = reason, 
                    term=self.state.current_term, 
                    last_log_index=self.state.get_last_log_index(), 
                    commit_index=self.commit_index
                )
                return reply()

            if self.state.get_term_at_index(prev_log_index) != prev_log_term: # self.state.log[self.state.global_to_local(prev_log_index)].term != prev_log_term:
                # Exists an entry at prev_log_index but terms mismatch
                success = False
                reason = 'terms mismatch'

                conflict_term = self.state.get_term_at_index(prev_log_index) # self.state.log[self.state.global_to_local(prev_log_index)].term
                conflict_term_first_index = self.state.get_term_first_index(conflict_term, prev_log_index)

                self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                    success=success, 
                    reason = reason, 
                    term=self.state.current_term, 
                    last_log_index=self.state.get_last_log_index(), 
                    commit_index=self.commit_index,
                    conflict_term = conflict_term,
                    conflict_term_first_index = conflict_term_first_index
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
            if index < self.state.get_last_log_index() + 1:
                if self.state.get_term_at_index(index) != entry.term: # self.state.log[self.state.global_to_local(index)].term != entry.term:
                    # Conflict: delete entry and all that follow it
                    reason = f'conflicting entry at index {index}'
                    self.state.log_truncate_suffix(index) # self.state.log = self.state.log[:index]
                    self.state.save_log_to_file()
                    conflict_index = index
                    # After truncation, fall through to append remaining entries
                    break 
                else:
                    reason = f'entries up to index {index} already existed'
                    conflict_index = index + 1
            elif index == self.state.get_last_log_index() + 1:
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
        self.state.append_log_entries_to_file(new_entries[start:])

        # Rule 5: Update commitIndex if leaderCommit > commitIndex
        if leader_commit > self.commit_index:
            last_new_entry_index = prev_log_index + len(new_entries)
            self.commit_index = min(leader_commit, last_new_entry_index)

        # Optionally, apply newly committed entries to state machine here or via separate async task
        # Apply commited entries routine for all
        if self.last_applied < self.commit_index:
            self.apply_committed_entries()

        success = True
        conflict_term = None
        conflict_term_first_index = None

        if new_entries:
            self.report(f'{self.role} {self.node_id} AppendEntries_processed', 
                success=success, 
                reason = reason, 
                term=self.state.current_term, 
                last_log_index=self.state.get_last_log_index(), 
                commit_index=self.commit_index
            )
        else:
            self.report(f'{self.role} {self.node_id} heartbeat_processed', 
                success=success, 
                reason = reason, 
                term=self.state.current_term, 
                last_log_index=self.state.get_last_log_index(), 
                commit_index=self.commit_index
            )

        return reply()

    async def send_message(self, peer_id, message):
        # Send a RPC message over TCP to a peer and handle response
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

    async def start_client_server(self):
        # TCP server that listens for incoming messages from clients
        # host, _ = self.address_book[self.node_id].split(':')
        host, port = lan_ip_addresses[self.node_id].split(':') # For incoming external client communication when port forwarding is set up. 
        # port = self.client_port
        server = await asyncio.start_server(self.handle_client_connection, host, int(port))

        async with server:
            self.report(f'{self.role} {self.node_id} client_server_started', host=host, port=port)
            await server.serve_forever()
        
    async def handle_client_connection(self, reader, writer):
        # Handle an incoming connection, read and respond to messages
        try:
            message = await read_message(reader)
            response = await self.handle_client_message(message)
            if response:
                writer.write(encode_message(response))
                await writer.drain()
        except Exception as e:
            self.report(f'{self.node_id} client_connection_error', error=str(e))
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_connection(self, reader, writer):
        # Handle an incoming connection, read and respond to messages
        try:
            while True:
                message = await read_message(reader)
                response = await self.handle_message(message)
                if response:
                    writer.write(encode_message(response))
                    await writer.drain()
        # except Exception as e:
        #     self.report(f'{self.node_id} connection_error', error=str(e))
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    def create_snapshot(self):
        '''Called by the node itself. Creates a new snapshot and installs it.'''
        snapshot_data = self.state_machine.get_snapshot()
        last_index = self.last_applied
        term = self.state.get_term_at_index(last_index) # self.state.log[self.state.global_to_local(last_index)].term 

        # Install snapshot
        self.state.install_snapshot(last_index, term, snapshot_data)
