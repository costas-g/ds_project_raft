import asyncio
import random
import time
import argparse
from raft.command import Command
from run.client import *

class PerformanceTester:
    def __init__(self, client: Client, commands_per_second: int, test_duration: int):
        self.client = client
        self.cps = commands_per_second
        self.duration = test_duration # seconds
        self.sent = 0
        self.success = 0
        self.latencies = []

    async def send_command(self, cmd_id):
        # generate a random command
        cmd_type = random.choice(Command.allowed_cmds[1:])
        key = random.choice(['a','b','c','d','e','f','g','h','i','j'])
        val = random.choice([0,1,2,3,4,5,6,7,8,9])
        cmd = Command(cmd_type, key, val)

        start = time.monotonic()
        try:
            response = await self.client.send_command(cmd)
            latency = time.monotonic() - start
            if response.from_leader and response.reply_message[0:2] == 'OK':
                self.success += 1
                self.latencies.append(latency)
            else:
                pass
        except Exception:
            pass

    async def run_test(self):
        interval = 1 / self.cps
        start_time = time.monotonic()
        while time.monotonic() - start_time < self.duration:
            asyncio.create_task(self.send_command(self.sent))
            self.sent += 1
            await asyncio.sleep(interval)
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

async def main(cps=100, duration=5, num_clients=1):
    # Instanciate the clients here
    clients = [Client(f'Benchmarker-{i}') for i in range(num_clients)]

    # Split total CPS across clients
    cps_per_client = cps // num_clients

    testers = [
        PerformanceTester(client=clients[i], commands_per_second=cps_per_client, test_duration=duration)
        for i in range(num_clients)
    ]

    # Run all testers concurrently
    await asyncio.gather(*(tester.run_test() for tester in testers))

    # Aggregate and report results
    total_sent = sum(t.sent for t in testers)
    total_success = sum(t.success for t in testers)
    total_latencies = [lat for t in testers for lat in t.latencies]

    total_throughput = total_success / duration
    avg_latency = sum(total_latencies) / len(total_latencies) if total_latencies else 0
    
    print(f"Num of Clients: {num_clients}")
    print(f"TOTAL SENT: {total_sent} commands")
    print(f"RECEIVED: {total_success} successes ({100*(total_success/total_sent):.2f}%)")
    print(f"Throughput: {total_throughput:.2f} cmd/s")
    print(f"Avg Latency: {avg_latency*1000:.4f} ms")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cps", type=int, default=100, help="Commands per second in total")
    parser.add_argument("--duration", type=int, default=10, help="Test duration in seconds")
    parser.add_argument("--clients", type=int, default=1, help="Number of clients")
    args = parser.parse_args()

    asyncio.run(main(args.cps, args.duration, args.clients))
