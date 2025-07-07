from typing import Dict

class InstallSnapshot:
    def __init__(self, term, leader_id, last_included_index, last_included_term, snapshot_data: Dict):
        self.term = term
        self.leader_id = leader_id
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.snapshot_data: Dict = snapshot_data  # dict type

    def to_dict(self):
        return {
            'type': 'InstallSnapshot',
            'term': self.term,
            'leader_id': self.leader_id,
            'last_included_index': self.last_included_index,
            'last_included_term': self.last_included_term,
            'snapshot_data': self.snapshot_data,
        }

    @staticmethod
    def from_dict(data):
        return InstallSnapshot(
            term=data['term'],
            leader_id=data['leader_id'],
            last_included_index=data['last_included_index'],
            last_included_term=data['last_included_term'],
            snapshot_data=data['snapshot_data'],
        )


class InstallSnapshotReply:
    def __init__(self, term, success, source_id):
        self.term = term
        self.success = success
        self.source_id = source_id

    def to_dict(self):
        return {
            'type': 'InstallSnapshotReply',
            'term': self.term,
            'success': self.success,
            'source_id': self.source_id,
        }

    @staticmethod
    def from_dict(data):
        return InstallSnapshotReply(
            term=data['term'],
            success=data['success'],
            source_id=data['source_id'],
        )
