class AppendEntries:
    def __init__(self, term, leader_id):
        self.term = term
        self.leader_id = leader_id

    def to_dict(self):
        return {
            'type': 'AppendEntries',
            'term': self.term,
            'leader_id': self.leader_id
        }

    @staticmethod
    def from_dict(d):
        return AppendEntries(d['term'], d['leader_id'])
