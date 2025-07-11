import asyncio
import random
import subprocess
from typing import Dict
from run.cluster_config import nodes
from run.run_cluster import run_node

PYTHON = "python"  # or "py" on Windows
CRASH_DELAY = 5     # seconds before crashing a node
DOWN_TIME = 5       # seconds node stays down
TEST_DURATION = 20  # total test time
CPS = 500
NUM_CLIENTS = 500

node_procs: Dict[str, asyncio.subprocess.Process] = {}
node_tasks: Dict[str, asyncio.Task] = {}

def start_node(node_id):
    print(f"Starting node {node_id}")
    task = asyncio.create_task(run_node(str(node_id)))
    node_tasks[node_id] = task

def kill_node(node_id: str):
    print(f"Killing node {node_id}")
    proc = node_procs.get(node_id)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
    node_procs[node_id] = None

def kill_node(node_id):
    task = node_tasks.get(node_id)
    if task and not task.done():
        print(f"Killing node {node_id}")
        task.cancel()

async def crash_cycle():
    await asyncio.sleep(CRASH_DELAY)
    node_to_kill = random.choice(nodes)
    kill_node(node_to_kill)
    await asyncio.sleep(DOWN_TIME)
    start_node(node_to_kill)

async def run_test():
    print("Launching cluster...")
    # Launch all nodes concurrently
    for i in nodes:
        start_node(i)

    await asyncio.sleep(10) # wait until a leader gets elected

    # Launch crash cycles in background
    asyncio.create_task(crash_cycle())
    # asyncio.create_task(crash_cycle())  # optional: more crashes

    # Run your performance test
    print("Starting load test...")
    proc = await asyncio.create_subprocess_exec(
        PYTHON, "-u", "-m", "tests.client_load",
        "--duration", str(TEST_DURATION),
        "--cps", str(CPS),
        "--clients", str(NUM_CLIENTS),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )

    # Print the output live
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        print(line.decode().rstrip())

    await proc.wait()

    print("Test complete.")

    print("Terminating cluster...")
    for n in nodes:
        kill_node(n)

    await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_test())
