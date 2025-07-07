import asyncio
import random
import time
from run.cluster_config import NUM_NODES, nodes, addresses, client_ports
from raft.messages.message import encode_message, read_message
from raft.messages.client_request import ClientRequest, ClientRequestResponse
from raft.command import Command

class Client:
    def __init__(self, client_id: str = 'Client'):
        self.client_id = client_id
        self.current_node_index: int = random.choice(range(NUM_NODES))
        self.current_node = nodes[self.current_node_index]
        self.next_command_id = 0
        self.timeout = 2    # seconds
        self.start_time = 0 # seconds
        self.end_time = 0   # seconds

    async def send_command(self, cmd: Command) -> ClientRequestResponse:
        self.next_command_id += 1
        req = ClientRequest(client_id=self.client_id, command_id=self.next_command_id, command=cmd)
        try:
            MAX_ITER = 5 # max number of times retrying the request
            iterations = 0
            # self.start_time = time.perf_counter()  # high-resolution timer

            while iterations < MAX_ITER: 
                response = await self._send_to_node(self.current_node, req)

                if response.from_leader:
                    #self.next_command_id += 1
                    # iterations and print()
                    return response#.result
                elif response.leader_id:
                    self.current_node = response.leader_id
                    response = await self._send_to_node(self.current_node, req)

                    if response:
                        #self.next_command_id += 1
                        # iterations and print()
                        return response#.result
                    else:
                        self.current_node_index = random.choice([x for x in range(0, NUM_NODES) if x != self.current_node_index])
                        self.current_node = nodes[self.current_node_index]
                        # print(f'debug: No response. Changing to node {self.current_node}.')
                        #return ClientRequestResponse(None, None, None, None, 'No response. Changing to a new random node.', None)
                else:
                    self.current_node_index = random.choice([x for x in range(0, NUM_NODES) if x != self.current_node_index])
                    self.current_node = nodes[self.current_node_index]
                    # print(f'debug: Not leader and no redirect. Changing to node {self.current_node}.')
                    #return ClientRequestResponse(None, None, None, None, "Not leader and no redirect. Changing to a new random node.", None)

                # print('.', end='', flush=True)#f'try {iterations}')
                iterations += 1
                iterations > 4 and print('failed on retry', iterations)

            # iterations and print()

            # # Calculate latency when no response
            # self.end_time = time.perf_counter()
            # latency_ms = (self.end_time - self.start_time) * 1000
            # print(f"[Latency] {latency_ms:.4f} ms")  # or log to a file
            return ClientRequestResponse(self.next_command_id, False, None, None, f"Tried to connect {iterations} times, no response, stop trying.", None)
        except asyncio.TimeoutError:
            # iterations and print()
            return ClientRequestResponse(self.next_command_id, False, None, None, "Request timed out", None)

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

            # Calculate latency when final response
            # if(msg['from_leader']):
            #     self.end_time = time.perf_counter()
            #     latency_ms = (self.end_time - self.start_time) * 1000
            #     print(f"[Latency] {latency_ms:.4f} ms")  # or log to a file

            return ClientRequestResponse.from_dict(msg)
        except Exception as e:
            # print(f'[cmd_id: {request.command_id}] debug: Exception: {e}')
            return ClientRequestResponse(request.command_id, False, 'exception', None, 'excepcion', None)
