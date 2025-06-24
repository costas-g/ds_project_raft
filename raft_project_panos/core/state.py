class RaftState:
    def __init__(self, node_id):
        self.node_id = node_id

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Κάθε entry μπορεί να είναι dict με {term, command}

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0

        # Leader-only volatile state
        self.next_index = {}  # peer_id -> index
        self.match_index = {}  # peer_id -> index

    def reset_leader_state(self, peer_ids):
        for peer in peer_ids:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = 0
#new
