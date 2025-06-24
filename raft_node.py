from raft_state import RaftState
from rpc.request_vote import RequestVote, RequestVoteReply
from rpc.append_entries import AppendEntries, AppendEntriesReply

class RaftNode:
    def __init__(self, node_id, state_file=None):
        self.node_id = node_id
        self.state_file = state_file or f'state_{node_id}.json'
        self.state = RaftState()
        self.state.load_from_file(self.state_file)

        self.role = 'follower'  # follower, candidate, or leader

        # Volatile state on all servers
        self.commit_index = -1    # highest log entry known to be committed
        self.last_applied = -1    # highest log entry applied to state machine

        # Volatile state on leaders
        self.next_index = {}      # for each follower, index of next log entry to send
        self.match_index = {}     # for each follower, highest log entry known replicated

        # Timers and networking will be added later

    def handle_request_vote(self, request: RequestVote) -> RequestVoteReply:
        if request.term < self.state.current_term:
            return RequestVoteReply(term=self.state.current_term, vote_granted=False)

        if request.term > self.state.current_term:
            self.become_follower(request.term)

        vote_granted = False
        if (self.state.voted_for is None or self.state.voted_for == request.candidate_id) and \
           self.is_log_up_to_date(request.last_log_index, request.last_log_term):
            self.state.voted_for = request.candidate_id
            vote_granted = True

        self.state.save_to_file(self.state_file)
        return RequestVoteReply(term=self.state.current_term, vote_granted=vote_granted)

    def handle_append_entries(self, request: AppendEntries) -> AppendEntriesReply:
        # TODO: implement AppendEntries logic
        pass

    def become_follower(self, term):
        self.role = 'follower'
        self.state.current_term = term
        self.state.voted_for = None
        # TODO: reset election timer, clear leader info

    def become_candidate(self):
        self.role = 'candidate'
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        # TODO: start election: send RequestVote RPCs, reset election timer

    def become_leader(self):
        self.role = 'leader'
        # TODO: initialize leader state (next_index, match_index)
        # send initial empty AppendEntries (heartbeat) to followers

    def send_request_vote(self):
        # TODO: send RequestVote RPCs to other nodes
        pass

    def send_append_entries(self):
        # TODO: send AppendEntries RPCs to followers
        pass

    def is_log_up_to_date(self, candidate_last_index, candidate_last_term):
        if not self.state.log:
            return True
        last_index = len(self.state.log) - 1
        last_term = self.state.log[-1]['term']
        if candidate_last_term > last_term:
            return True
        if candidate_last_term == last_term and candidate_last_index >= last_index:
            return True
        return False
