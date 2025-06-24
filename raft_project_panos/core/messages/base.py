class RaftMessage:
    def __init__(self, term, sender_id):
        self.term = term
        self.sender_id = sender_id
