import asyncio
import sys
from run.cluster_config import addresses, client_ports
from raft.command import Command
from run.client import Client

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
            if len(cmd_arg_list) > 1:
                key = cmd_arg_list[1]
            if len(cmd_arg_list) > 2:
                val = cmd_arg_list[2]

            cmd = Command(cmd_type, key, val)
            if not cmd:
                continue
            response = await client.send_command(cmd)
            print(f'Result: {response.result} ({response.reply_message})')
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
