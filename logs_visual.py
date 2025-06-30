import os
import json
import time

STATE_DIR = 'states'
REFRESH_INTERVAL = 1.0  # seconds

def load_logs():
    logs = {}
    for fname in sorted(os.listdir(STATE_DIR)):
        if not fname.startswith('state_') or not fname.endswith('.json'):
            continue
        node_id = fname.replace('state_', '').replace('.json', '')
        try:
            with open(os.path.join(STATE_DIR, fname), 'r') as f:
                data = json.load(f)
                log_terms = [entry['term'] for entry in data.get('log', [])]
                logs[node_id] = log_terms
        except: # (json.JSONDecodeError, OSError):
            # Skip this file this cycle if it's currently being written
            continue
    return logs

def print_logs(logs):
    # Determine max log length
    max_len = max((len(log) for log in logs.values()), default=0)

    # Header with indexes
    header = '--i-' + ''.join(f'-{i:03}-' for i in range(max_len))
    print(header)

    for node_id in sorted(logs.keys()):
        row = f'[{node_id}]'
        row += ''.join(f'-{term:03}-' for term in logs[node_id])

        print(row)

    # Blank line for spacing
    print()

    # Print each node’s current_term on its own line
    for node_id in sorted(logs.keys()):
        try:
            with open(os.path.join('states', f'state_{node_id}.json')) as f:
                data = json.load(f)
                current_term = data.get('current_term', '?')
        except:
            current_term = '?'

        print(f'[{node_id}] current_term = {current_term}')


def monitor_logs():
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            logs = load_logs()
            print_logs(logs)
            time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    monitor_logs()
