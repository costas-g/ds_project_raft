import asyncio
import random
import time
from run.cluster_config import NUM_NODES, nodes, addresses, remote_addresses
from raft.messages.message import encode_message, read_message
from raft.messages.client_request import ClientRequest, ClientRequestResponse
from raft.command import Command

class Client:
    def __init__(self, client_id: str = 'Client'):
        self.client_id = client_id
        self.current_node_index: int = random.choice(range(NUM_NODES))
        self.current_node = nodes[self.current_node_index]
        self.next_command_id = 0
        self.timeout = 0.5    # seconds
        self.start_time = 0 # seconds
        self.end_time = 0   # seconds

    async def send_command(self, cmd: Command) -> ClientRequestResponse:
        MAX_TRIES = 5
        self.next_command_id += 1
        req = ClientRequest(client_id=self.client_id, command_id=self.next_command_id, command=cmd)

        for attempt in range(MAX_TRIES):
            try:
                response = await self._send_to_node(self.current_node, req)
                
                if response is None:
                    # Rotate to next node and continue retry
                    self.current_node_index = (self.current_node_index + 1) % len(nodes)
                    self.current_node = nodes[self.current_node_index]
                    #await asyncio.sleep(0.001)
                    #print(f'response None')
                    continue

                if response.from_leader:
                    return response

                elif response.leader_id:
                    self.current_node = response.leader_id
                    # Retry immediately on the new leader within the same attempt
                    response = await self._send_to_node(self.current_node, req)
                    if response.from_leader:
                        return response
                    # If still no success, continue to next iteration for retry

                # Rotate to next node on failure or no leader info
                self.current_node_index = (self.current_node_index + 1) % len(nodes)
                self.current_node = nodes[self.current_node_index]

            except asyncio.TimeoutError:
                # Optionally log or handle timeout
                pass

            # Backoff to avoid tight retry loop
            #await asyncio.sleep(0.001)
            #print(f'retry to {self.current_node}')

        # After max retries
        print('.', end='', flush=True)
        return ClientRequestResponse(
            self.next_command_id,
            False,
            None,
            None,
            f"Tried {MAX_TRIES} times, no success.",
            None
        )
   
    
    async def _send_to_node(self, node_id, request: ClientRequest) -> ClientRequestResponse:
        host, port = remote_addresses[node_id].split(':')
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=self.timeout)
            writer.write(encode_message(request.to_dict()))
            await writer.drain()
            msg = await asyncio.wait_for(read_message(reader), timeout=self.timeout)
            writer.close()
            await writer.wait_closed()
            return ClientRequestResponse.from_dict(msg)
        except (asyncio.TimeoutError, ConnectionError) as e:
            # Connection level failures: indicate node unreachable
            # Consider raising or returning None for retry logic to switch nodes
            # print(f'Connection error to node {node_id}: {e}')
            return None
        except Exception as e:
            # print(f'RPC error: {e}')
            return ClientRequestResponse(request.command_id, False, 'exception', None, 'exception', None)
