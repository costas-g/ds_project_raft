import unittest
from rpc.append_entries import AppendEntries, AppendEntriesReply

class TestAppendEntries(unittest.TestCase):
    def test_request_serialization(self):
        req = AppendEntries(
            term=2,
            leader_id='leader1',
            prev_log_index=5,
            prev_log_term=2,
            entries=[{'term': 2, 'command_id': 'cmd42'}],
            leader_commit=4
        )
        json_str = req.to_json()
        restored = AppendEntries.from_json(json_str)
        self.assertEqual(restored.term, req.term)
        self.assertEqual(restored.leader_id, req.leader_id)
        self.assertEqual(restored.prev_log_index, req.prev_log_index)
        self.assertEqual(restored.prev_log_term, req.prev_log_term)
        self.assertEqual(restored.entries, req.entries)
        self.assertEqual(restored.leader_commit, req.leader_commit)

    def test_reply_serialization(self):
        reply = AppendEntriesReply(term=2, success=True)
        json_str = reply.to_json()
        restored = AppendEntriesReply.from_json(json_str)
        self.assertEqual(restored.term, reply.term)
        self.assertEqual(restored.success, reply.success)

if __name__ == '__main__':
    unittest.main()
