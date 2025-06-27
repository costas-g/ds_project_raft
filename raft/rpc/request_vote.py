class RequestVote:
    def __init__(self, term, candidate_id):
        self.term = term
        self.candidate_id = candidate_id

    def to_dict(self):
        return {
            'type': 'RequestVote',
            'term': self.term,
            'candidate_id': self.candidate_id
        }

    @staticmethod
    def from_dict(d):
        return RequestVote(d['term'], d['candidate_id'])
