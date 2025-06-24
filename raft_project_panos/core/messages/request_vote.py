from .base import RaftMessage

class RequestVote(RaftMessage):
    def __init__(self, term, candidate_id):
        super().__init__(term, candidate_id)
        self.candidate_id = candidate_id

class RequestVoteResponse(RaftMessage):
    def __init__(self, term, sender_id, vote_granted):
        super().__init__(term, sender_id)
        self.vote_granted = vote_granted
