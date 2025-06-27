class RequestVote:
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
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
    def __init__(self, term, vote_granted):
        self.term = term
        self.vote_granted = vote_granted

    def to_dict(self):
        return {
            'type': 'RequestVoteReply',
            'term': self.term,
            'vote_granted': self.vote_granted
        }

    @staticmethod
    def from_dict(d):
        return RequestVoteReply(d['term'], d['vote_granted'])
