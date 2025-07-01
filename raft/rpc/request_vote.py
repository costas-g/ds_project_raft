class RequestVote:
    def __init__(
        self, 
        term: int, 
        candidate_id: str, 
        last_log_index: int, 
        last_log_term: int
    ):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def to_dict(self):
        return {
            'type': 'RequestVote',
            'term': self.term,
            'candidate_id': self.candidate_id,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term,
        }

    @staticmethod
    def from_dict(d):
        return RequestVote(d['term'], d['candidate_id'], d['last_log_index'], d['last_log_term'])


class RequestVoteReply:
    def __init__(self, term: int, vote_granted: bool, source_id: str):
        self.term = term
        self.vote_granted = vote_granted
        self.source_id = source_id

    def to_dict(self):
        return {
            'type': 'RequestVoteReply',
            'term': self.term,
            'vote_granted': self.vote_granted,
            'source_id': self.source_id
        }

    @staticmethod
    def from_dict(d):
        return RequestVoteReply(
            d['term'], 
            d['vote_granted'], 
            d['source_id']
        )
