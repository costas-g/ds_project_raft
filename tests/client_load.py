import asyncio
import random
import string
import time
import argparse
from raft.command import Command
from run.client import *

# import psutil
# psutil.Process().cpu_affinity([0])

MAX_CMDS_GEN = 1000

class PerformanceTester:
    def __init__(self, client: Client, commands_per_second: float, test_duration: int, global_start_time):
        self.client = client
        self.cps = commands_per_second
        self.duration = test_duration # seconds
        self.global_start_time = global_start_time
        self.sent = 0
        self.success = 0
        self.latencies = []
        self.commands_to_send = []
        self.generate_commands(MAX_CMDS_GEN)

    def generate_commands(self, num_commands):
        for c in range(num_commands):
            # generate a random command
            cmd_type = random.choice(Command.allowed_cmds[1:])
            key = random.choice(list(string.ascii_lowercase[:26]))
            val = random.choice(list(range(10)))
            cmd = Command(cmd_type, key, val)
            self.commands_to_send.append(cmd)

    async def send_command(self, cmd_id: int):
        # # generate a random command
        # cmd_type = 'read' #random.choice(Command.allowed_cmds[1:])
        # key = random.choice(list(string.ascii_lowercase[:26]))
        # val = random.choice(list(range(10)))
        # cmd = Command(cmd_type, key, val)

        cmd = self.commands_to_send[cmd_id%MAX_CMDS_GEN]

        start = time.monotonic()
        try:
            response = await self.client.send_command(cmd)
            latency = time.monotonic() - start

            if response is None:
                raise RuntimeError("send_command() returned None")
            
            if response.from_leader and response.reply_message[0:2] == 'OK':
                self.success += 1
                self.latencies.append(latency)
            else:
                #print('failed')
                #print(response.result, response.reply_message)
                pass
        except Exception as e:
            print('#', end='', flush=True)
            # print(f'oops: {e}')
            pass

    async def run_test(self):
        # Wait until global_start_time
        now = time.monotonic()
        delay = max(0, self.global_start_time - now)
        await asyncio.sleep(delay)

        interval = 1 / self.cps

        # same end time for all testers
        end_time = self.global_start_time + self.duration + interval

        # initial random wait to avoid too many clients overloading the leader at once
        await asyncio.sleep(random.uniform(0, interval)) 

        while time.monotonic() < end_time:
            # Start sending commands only after the maximum random wait time above.
            # On average, the load has a steady increase initially until peak. 
            # The smaller the interval the less noticeable the increase is. 
            if time.monotonic() > self.global_start_time + interval:
                asyncio.create_task(self.send_command(self.sent))
                self.sent += 1
            # extra randomization in wait time between commands 
            # to reduce the chances of synchronized client requests that can overwhelm the leader
            # and simulate more realistic client behavior
            await asyncio.sleep(random.uniform(0.5*interval, 1.5*interval))
        # Wait for all commands to finish
        await asyncio.sleep(2)

    def report(self, cps, T):
        total_time = self.duration
        throughput = self.success / total_time
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        print(f"[CPS: {cps}, T: {T}] Sent: {self.sent}, Success: {self.success}, Throughput: {throughput:.2f} cmd/s, Avg latency: {avg_latency*1000:.4f} ms")

# async def main(cps = 100, duration = 5):
#     # Place the client here
#     client = Client('Benchmarker')
#     tester = PerformanceTester(client=client, commands_per_second=cps, test_duration=duration)
#     await tester.run_test()
#     tester.report(cps, duration)

#for multiprocessing clients
def run_test_worker(client_id, cps, duration, global_start_time):
    async def _runner():
        client = Client(client_id)
        tester = PerformanceTester(client, cps, duration, global_start_time)
        await tester.run_test()
        return tester.sent, tester.success, tester.latencies

    return asyncio.run(_runner())

async def main(cps=100, duration=5, num_clients=1):
    # Split total CPS across clients
    cps_per_client = cps / num_clients # allow less than 1 cps per client

    # # multiprocessing
    # global_start_time = time.monotonic() + 10
    # with ProcessPoolExecutor(max_workers=4) as executor:
    #     futures = [
    #         executor.submit(run_test_worker, f'Benchmarker-{i}', cps_per_client, duration, global_start_time)
    #         for i in range(num_clients)
    #     ]
    #     results = [f.result() for f in futures]

    # Instantiate the clients
    clients = [Client(f'Benchmarker-{i}') for i in range(num_clients)]

    # Set global start time at 1 second after starting the instantiation of all testers. 
    # Even some thousand servers need only a few miliseconds to be set and ready, so 1 second is enough. 
    global_start_time = time.monotonic() + 5

    # Instantiate the testers
    testers = [
        PerformanceTester(client=clients[i], commands_per_second=cps_per_client, test_duration=duration, global_start_time=global_start_time)
        for i in range(num_clients)
    ]

    if time.monotonic() > global_start_time:
        print('TEST ABORTED, WAIT MORE TIME FOR TESTERS TO LOAD')
        return

    # Start all testers, but they'll wait until global_start_time before they actually start sending commands. 
    tasks = [asyncio.create_task(t.run_test()) for t in testers]
    # Wait for all to finish
    await asyncio.gather(*tasks)

    real_end_time = time.monotonic()
    print(f'\nTotal elapsed time: {(real_end_time - global_start_time):.1f} s')

    # Aggregate and report results
    total_sent = sum(t.sent for t in testers)
    total_success = sum(t.success for t in testers)
    total_latencies = [lat for t in testers for lat in t.latencies]
    # with multiprocessing
    # total_sent = sum(r[0] for r in results)
    # total_success = sum(r[1] for r in results)
    # total_latencies = [lat for r in results for lat in r[2]]

    total_throughput = total_success / duration
    avg_latency = sum(total_latencies) / len(total_latencies) if total_latencies else 0
    max_latency = max(total_latencies) if total_latencies else 0

    print(f"Num of Clients: {num_clients}")
    print(f"TOTAL SENT: {total_sent} commands")
    print(f"RECEIVED: {total_success} successes ({100*(total_success/total_sent):.2f}%)")
    print(f"Throughput: {total_throughput:.1f} cmd/s")
    print(f"Avg Latency: {avg_latency*1000:.2f} ms (max: {max_latency*1000:.2f} ms)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cps", type=int, default=100, help="Commands per second in total")
    parser.add_argument("--duration", type=int, default=10, help="Test duration in seconds")
    parser.add_argument("--clients", type=int, default=1, help="Number of clients")
    args = parser.parse_args()

    asyncio.run(main(args.cps, args.duration, args.clients))
