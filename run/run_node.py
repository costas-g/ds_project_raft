import asyncio
import json
import os
import logging
from raft.raft_node import RaftNode
import run.cluster_config as config
import psutil

def validate_config():
    required_timing_keys = ["heartbeat_interval", "election_timeout_min", "election_timeout_max"]

    if not hasattr(config, "timing"):
        raise RuntimeError("Missing 'timing' section in config")

    for key in required_timing_keys:
        if key not in config.timing:
            raise RuntimeError(f"Missing timing parameter: {key}")

    if not hasattr(config, "nodes") or not config.nodes:
        raise RuntimeError("No nodes defined in config")

    if not hasattr(config, "addresses"):
        raise RuntimeError("Missing 'addresses' in config")

    # if not hasattr(config, "client_ports"):
    #     raise RuntimeError("Missing 'client_ports' in config")

def setup_logger(node_id):
    # Setup per-node logger writing to logs/<node_id>.log
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/{node_id}.log")
    formatter = logging.Formatter('%(asctime)s [%(name)s] -- %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def event_logger(node_id, event, data):
    # Event callback passed to RaftNode for logging events
    logger = logging.getLogger(node_id)
    msg = f"{event}: {data}"
    logger.info(msg)

async def monitor_cpu():
    proc = psutil.Process(os.getpid())
    while True:
        cpu = proc.cpu_percent(interval=None)
        print(f"[CPU] {cpu:.2f}%")
        await asyncio.sleep(1)

async def main(node_id):
    # Validate config and start node event loop
    validate_config()
    peers = [n for n in config.nodes if n != node_id]
    timing = config.timing

    # Setup logger FIRST, so it exists before any logging
    logger = setup_logger(node_id)
    logger.info(f"-----------------------")
    logger.info(f"-----------------------")
    logger.info(f"Node {node_id} starting")

    node = RaftNode(
        node_id,
        peers,
        config.addresses,
        # client_port=config.client_ports[node_id],
        event_callback=event_logger,
        batching_interval=timing["batching_interval"],
        heartbeat_interval=timing["heartbeat_interval"],
        election_timeout_min=timing["election_timeout_min"],
        election_timeout_max=timing["election_timeout_max"],
    )
    
    asyncio.create_task(monitor_cpu())  # launch the CPU monitor

    await node.start()

    # Run until interrupted, then shutdown cleanly
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await node.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run_node.py <node_id>")
        exit(1)
    node_id = sys.argv[1]

    if node_id not in config.nodes:
        print(f"Error: node_id '{node_id}' not found in config.nodes")
        exit(1)

    try:
        asyncio.run(main(node_id))
    except KeyboardInterrupt:
        pass  # Allow graceful shutdown on Ctrl+C
