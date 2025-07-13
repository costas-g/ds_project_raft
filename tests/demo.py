import subprocess
import sys
import os
import asyncio
import json
from run.cluster_config import nodes
from tests.client_load import main as run_performance_test
from run.client import Client
from raft.command import Command

NODE_IDS = nodes #['n1', 'n2', 'n3']  # Adjust as needed
RUN_NODE_SCRIPT = os.path.abspath('run/run_node.py')
i=0

async def run_node(node_id):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-m", "run.run_node", node_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    return proc

async def get_leader_id():
    # Use the random client logic to send a dummy read command
    client = Client("DemoQuerier")
    cmd = Command("read", "foo", None)  # key "foo" can be anything, or even non-existent
    response = await client.send_command(cmd)
    # Response will have .leader_id (could be None if no leader yet)
    return response.leader_id


async def main():    
    # Start all nodes and keep their procs!
    procs = {}    
    #pref_task_1 = asyncio.create_task(run_performance_test(cps=2000, duration=10, num_clients=2))
    
    try:
        for node in NODE_IDS:
            procs[node] = await run_node(node)
            print(f"Started {node} with PID {procs[node].pid}")    
        print("Waiting for cluster to stabilize...\n")
        await asyncio.sleep(10)
        
        # Multiple test scenarios/ can modify as we like
        print("Beginning tests with different parameters")        
        print("Starting baseline performance test...\n")
        await run_performance_test(cps=500, duration=10, num_clients=500)
        await asyncio.sleep(5)
        
        print("\nContinue with a more complicated performance test...")
        print("With parameters of a 2000cps 10 sec duration and different number of clients")
        print("Keep in mind we wait couple sec between tests for stabilization\n")
        await run_performance_test(cps=2000, duration=10, num_clients=2)
        
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        await run_performance_test(cps=2000, duration=10, num_clients=4)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        await run_performance_test(cps=2000, duration=10, num_clients=6)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        await run_performance_test(cps=2000, duration=10, num_clients=8)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        await run_performance_test(cps=2000, duration=10, num_clients=12)
        
        print("----------------------------------\n")
        await asyncio.sleep(10)
        
        print("Beginning test with 2000cps 10sec duration and 2 clients")
        print("While killing the leader of the cluster for approx 3 secs\n")
        leader_id = await get_leader_id()
        await asyncio.sleep(3)
        if leader_id in procs:
            perf_task = asyncio.create_task(run_performance_test(cps=2000, duration=10, num_clients=2))
            await asyncio.sleep(2)
            procs[leader_id].terminate()
            await procs[leader_id].wait()
            print(f"Leader {leader_id} terminated.")
            await asyncio.sleep(3)
            procs[leader_id] = await run_node(leader_id)
            print(f"Leader {leader_id} restarted.")
            await perf_task
        else:
            print("No leader found to kill.\n")
        
        print("----------------------------------\n")
        await asyncio.sleep(10)
        print("Continuing with same test parameters")
        print("But now we kill a follower completely and 3 secs after start of the test...\n")
        follower_id = next(n for n in NODE_IDS if n != leader_id)
        if follower_id in procs:
            perf_task2 = asyncio.create_task(run_performance_test(cps=2000, duration=10, num_clients=2))
            await asyncio.sleep(3)
            procs[follower_id].terminate()
            await procs[follower_id].wait()
            print(f"Follower {follower_id} terminated.")
            await perf_task2
        else:
            print("No follower found to kill.")
        
    except asyncio.CancelledError:
        print("Test was cancelled (probably Ctrl+C)")
    finally:
        for proc in procs.values():
            if proc.returncode is None:
                #print(f"Terminating {node}...")
                proc.terminate()            
                await proc.wait()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Shutting down cluster due to keyboard interrupt.')
        pass
