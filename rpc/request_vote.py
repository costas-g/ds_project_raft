import json

class RequestVote:
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return RequestVote(**obj)


class RequestVoteReply:
    def __init__(self, term, vote_granted):
        self.term = term
        self.vote_granted = vote_granted

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return RequestVoteReply(**obj)


#new +++
#newwww
