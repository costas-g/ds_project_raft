import subprocess
import sys
import os
import asyncio
from run.cluster_config import nodes
import psutil

NODE_IDS = nodes #['n1', 'n2', 'n3']  # Adjust as needed
RUN_NODE_SCRIPT = os.path.abspath('run/run_node.py')

async def run_node(node_id):
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-u", "-m", "run.run_node", node_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # # SET A SPECIFIC CORE BASED ON node_id
    # p = psutil.Process(proc.pid)
    # p.cpu_affinity([1,2,3])  # pin to CPU core 0 [1,2,3] [int(node_id[1])]

    async def read_stream(stream, prefix):
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                print(f'[{prefix}] {line.decode().rstrip()}')
        except:
            pass

    # Launch the reading tasks immediately
    stdout_task = asyncio.create_task(read_stream(proc.stdout, node_id))
    stderr_task = asyncio.create_task(read_stream(proc.stderr, node_id))

    try:
        await proc.wait()
        await asyncio.gather(stdout_task, stderr_task)
    except:
        # Cancel stream readers if we get cancelled
        stdout_task.cancel()
        stderr_task.cancel()
        try:
            await asyncio.gather(stdout_task, stderr_task)
        except asyncio.CancelledError:
            pass
        # Terminate subprocess gracefully
        proc.terminate()
        await proc.wait()
        raise

async def main():
    tasks = [run_node(node) for node in NODE_IDS]
    try:
        await asyncio.gather(*tasks)
    except:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except:
        print('Shutting down cluster')
        pass
    
