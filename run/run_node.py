import asyncio
import json
import os
import logging
from raft.raft_node import RaftNode

def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'cluster_config.json')) as f:
        config = json.load(f)

    timing = config.get("timing")
    if timing is None:
        raise RuntimeError("Missing 'timing' section in config")

    required_keys = ["heartbeat_interval", "election_timeout_min", "election_timeout_max"]
    for key in required_keys:
        if key not in timing:
            raise RuntimeError(f"Missing timing parameter: {key}")

    return config

def setup_logger(node_id):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/{node_id}.log")
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def event_logger(node_id, event, data):
    logger = logging.getLogger(node_id)
    msg = f"{event}: {data}"
    logger.info(msg)

async def main(node_id):
    config = load_config()
    peers = [n for n in config["nodes"] if n != node_id]
    timing = config["timing"]

    node = RaftNode(
        node_id,
        peers,
        config["addresses"],
        event_callback=event_logger,
        heartbeat_interval=timing["heartbeat_interval"],
        election_timeout_min=timing["election_timeout_min"],
        election_timeout_max=timing["election_timeout_max"],
    )
    
    logger = setup_logger(node_id)
    logger.info(f"Node {node_id} starting")

    await node.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run_node.py <node_id>")
        exit(1)
    node_id = sys.argv[1]
    asyncio.run(main(node_id))
