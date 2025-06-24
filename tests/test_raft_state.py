import unittest
from raft_state import RaftState

class TestRaftState(unittest.TestCase):
    def test_save_load(self):
        state = RaftState()
        state.current_term = 5
        state.voted_for = 'node1'
        state.log = [{'term': 3, 'command_id': 'cmd123'}]

        filename = 'state.json'
        state.save_to_file(filename)

        # Load into a new instance to verify
        loaded_state = RaftState()
        loaded_state.load_from_file(filename)

        self.assertEqual(loaded_state.current_term, state.current_term)
        self.assertEqual(loaded_state.voted_for, state.voted_for)
        self.assertEqual(loaded_state.log, state.log)

if __name__ == '__main__':
    unittest.main()
