import unittest
from rpc.request_vote import RequestVote, RequestVoteReply

class TestRequestVote(unittest.TestCase):
    def test_request_serialization(self):
        msg = RequestVote(
            term=1, 
            candidate_id='node1', 
            last_log_index=10, 
            last_log_term=1
        )
        json_str = msg.to_json()
        restored = RequestVote.from_json(json_str)
        
        self.assertEqual(restored.term, msg.term)
        self.assertEqual(restored.candidate_id, msg.candidate_id)
        self.assertEqual(restored.last_log_index, msg.last_log_index)
        self.assertEqual(restored.last_log_term, msg.last_log_term)

    def test_reply_serialization(self):
        reply = RequestVoteReply(
            term=1, 
            vote_granted=True
        )
        json_str = reply.to_json()
        restored = RequestVoteReply.from_json(json_str)
        
        self.assertEqual(restored.term, reply.term)
        self.assertEqual(restored.vote_granted, reply.vote_granted)

if __name__ == '__main__':
    unittest.main()


#kainourgio comment
