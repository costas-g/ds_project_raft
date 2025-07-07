import asyncio
import sys
from run.cluster_config import addresses, client_ports
from raft.command import Command
from run.client import Client
import time

async def main(client_id):
    client = Client(client_id) if client_id else Client()
    print(f"Connected to Raft cluster as {client.client_id}")
    print(f"Selected Raft Node {client.current_node} at {addresses[client.current_node].split(':')[0]}:{client_ports[client.current_node]}")
    print("Type commands to send:")
    while True:
        try:
            command_str = input(f"({client.client_id} to {client.current_node})[CMD_ID: {client.next_command_id}]>>> ").strip()
            cmd_type = None
            key = None
            val = None

            tasks = []
            commands = []

            cmd_arg_list = command_str.split(' ')
            if len(cmd_arg_list) > 3:
                print(f'Usage: <command_type> <key> <value> (example: create x 5)')
                continue
            if not command_str:
                continue
            if len(cmd_arg_list) > 0:
                cmd_type = cmd_arg_list[0]
                if cmd_type == 'exit':
                    print("Goodbye")
                    break
                if cmd_type == 'pipe':
                    import random
                    for i in range(40):
                        cmd_type = random.choice(Command.allowed_cmds[1:])
                        key = random.choice(['a','b','c','d','e','f','g','h','i','j'])
                        val = random.choice([1,2,3,4,5,6,7,8,9,10])
                        cmd = Command(cmd_type, key, val)
                        commands.append(cmd)
                    
                    #return responses
            if len(cmd_arg_list) > 1:
                key = cmd_arg_list[1]
            if len(cmd_arg_list) > 2:
                val = cmd_arg_list[2]

            cmd = Command(cmd_type, key, val)
            if not cmd:
                continue

            commands.append(cmd) if cmd_type != 'pipe' else None
            
            for cmd in commands:
                tasks.append(asyncio.create_task(client.send_command(cmd)))
                
            start_time = time.monotonic()
            responses = await asyncio.gather(*tasks)
            end_time = time.monotonic()
            for response in responses:
                #response = await client.send_command(cmd)
                print(f'[{response.command_id:03d}] Result: {response.result} ({response.reply_message})')
            latency = end_time - start_time
            print(f'[Latency] {1000*latency:.4f} ms')

        except (KeyboardInterrupt, EOFError):
            print("\nInterrupted by user")
            break

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python client.py <client_id>")
        exit(1)
    
    client_id = None
    if len(sys.argv) == 2:
        client_id = sys.argv[1]

    try:
        asyncio.run(main(client_id))
    except KeyboardInterrupt:
        pass  # Allow graceful shutdown on Ctrl+C
