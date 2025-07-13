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
    procs: dict[str, asyncio.subprocess.Process] = {}    
    #pref_task_1 = asyncio.create_task(run_performance_test(cps=2000, duration=10, num_clients=2))
    
    try:
        #'''
        for node in NODE_IDS:
            procs[node] = await run_node(node)
            print(f"Started {node} with PID {procs[node].pid}")    
        print("Waiting for cluster to stabilize...\n")
        await asyncio.sleep(10)
        
        # Multiple test scenarios/ can modify as we like
        print("Beginning tests with different parameters")        
        print("Starting baseline performance test...\n")
        cps, duration, num_clients = 100, 10, 1
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        await asyncio.sleep(5)
        #'''
        print("\nContinue with a more complicated performance test...")
        print("With parameters of a 700cps 10 sec duration and different number of clients")
        print("Keep in mind we wait couple sec between tests for stabilization\n")
        cps, duration, num_clients = 700, 10, 50
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        cps, duration, num_clients = 700, 10, 100
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        cps, duration, num_clients = 700, 10, 500
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        cps, duration, num_clients = 700, 10, 700
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        cps, duration, num_clients = 700, 10, 700
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        cps, duration, num_clients = 700, 10, 1000
        print('cps:', cps, ', duration:', duration, ', num clients:', num_clients)
        await run_performance_test(cps, duration, num_clients)
        #'''
        print("----------------------------------\n")
        await asyncio.sleep(5)
        
        print("Beginning test with 700cps 10sec duration and 100 clients")
        print("While killing the leader of the cluster for approx 3 secs\n")
        leader_id = await get_leader_id()
        await asyncio.sleep(3)
        if leader_id in procs:
            perf_task = asyncio.create_task(run_performance_test(cps=700, duration=10, num_clients=100))
            await asyncio.sleep(2)
            procs[leader_id].kill()
            await procs[leader_id].wait()
            print(f"Leader {leader_id} terminated.")
            await asyncio.sleep(3)
            procs[leader_id] = await run_node(leader_id)
            print(f"Leader {leader_id} restarted.")
            await perf_task
        else:
            print("No leader found to kill.\n")
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        print("Continuing with same test parameters")
        print("But now we kill a follower completely and 3 secs after start of the test...\n")
        leader_id = await get_leader_id()
        await asyncio.sleep(3)        
        follower_id = next(n for n in NODE_IDS if n != leader_id)        
        if follower_id in procs:
            perf_task2 = asyncio.create_task(run_performance_test(cps=700, duration=10, num_clients=100))
            await asyncio.sleep(3)
            procs[follower_id].kill()
            await procs[follower_id].wait()
            print(f"Follower {follower_id} terminated.")
            await perf_task2
        else:
            print("No follower found to kill.")
        
        print("----------------------------------\n") 
        print("Restarting the follower.....\n")   
        procs[follower_id] = await run_node(follower_id)
        print(f"Follower {follower_id} restarted.")
        print("Waiting for cluster to stabilize...\n")
        await asyncio.sleep(15)
        
        
        print("----------------------------------\n")
        print("Continuing tests with fixed 100 clients but different cps")
        print("\n500cps")
        await run_performance_test(cps=500, duration=10, num_clients=100)
        print("----------------------------------\n")
        await asyncio.sleep(5)
        #'''
        print("700cps")
        await run_performance_test(cps=700, duration=10, num_clients=100)
        print("----------------------------------\n")
        await asyncio.sleep(5)
        
        print("800cps")
        await run_performance_test(cps=800, duration=10, num_clients=100)
        print("----------------------------------\n")
        await asyncio.sleep(5)
        
        print("900cps")
        await run_performance_test(cps=900, duration=10, num_clients=100)
        print("----------------------------------\n")
        await asyncio.sleep(10)
        
        print("----------------------------------\n")
        print("Continuing tests with fixed 700cps and 100 clients but different durations")
        print("10s")
        await run_performance_test(cps=700, duration=10, num_clients=100)  
        
        print("----------------------------------\n")
        await asyncio.sleep(5)
        print("Continuing tests with fixed 700cps and 100 clients but different durations")
        print("30s")
        await run_performance_test(cps=700, duration=30, num_clients=100)   
        
        print("----------------------------------\n")
        await asyncio.sleep(10)
        print("Continuing tests with fixed 700cps and 100 clients but different durations")
        print("60s")
        await run_performance_test(cps=700, duration=60, num_clients=100)
        #'''
        print("----------------------------------\n")
        await asyncio.sleep(10)
        print("Tests completed thank you for your patience!")
        
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
