import asyncio
import random
import sys
from run.cluster_config import nodes, addresses, client_ports
from raft.messages.message import encode_message, read_message
from raft.messages.client_request import ClientRequest, ClientRequestResponse
from raft.command import Command

class Client:
    def __init__(self, client_id: str = 'Client'):
        self.client_id = client_id
        self.current_node = random.choice(nodes)
        self.next_command_id = 0
        self.timeout = 3  # seconds

    async def send_command(self, command_str: Command) -> ClientRequestResponse:
        self.next_command_id += 1
        req = ClientRequest(client_id=self.client_id, command_id=self.next_command_id, command=command_str)
        try:
            response = await self._send_to_node(self.current_node, req)
            if response.from_leader:
                return response#.result
            elif response.leader_id:
                self.current_node = response.leader_id
                response = await self._send_to_node(self.current_node, req)
                if response:
                    return response#.result
                else:
                    return ClientRequestResponse(None, None, None, None, 'No response', None)
            else:
                self.current_node = random.choice(nodes)
                return ClientRequestResponse(None, None, None, None, "Not leader and no redirect. Changing to a random node.", None)
            
        except asyncio.TimeoutError:
            return ClientRequestResponse(None, None, None, None, "Request timed out", None)

    async def _send_to_node(self, node_id, request: ClientRequest) -> ClientRequestResponse:
        host = addresses[node_id].split(':')[0]
        port = client_ports[node_id]
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=self.timeout)
            writer.write(encode_message(request.to_dict()))
            await writer.drain()
            msg = await asyncio.wait_for(read_message(reader), timeout=self.timeout)
            writer.close()
            await writer.wait_closed()
            return ClientRequestResponse.from_dict(msg)
        except Exception as e:
            print(f'debug: Exception: {e}')
            return ClientRequestResponse(None, False, 'exception', None, 'excepcion', None)

async def main(client_id):
    client = Client(client_id) if client_id else Client()
    print(f"Connected to Raft cluster as {client.client_id}")
    print(f"Connected to Raft Node {client.current_node} at {addresses[client.current_node]}")
    print("Type commands to send:")
    while True:
        try:
            command_str = input(f"({client.client_id} to {client.current_node})[CMD_ID: {client.next_command_id+1}]>>> ").strip()
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
                    print("\nGoodbye")
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
            print("\nExiting.")
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
