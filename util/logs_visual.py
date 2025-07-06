import os
import json
import time

STATE_DIR = 'states'
REFRESH_INTERVAL = 1.0  # seconds

def load_logs():
    logs = {}
    for fname in sorted(os.listdir(STATE_DIR)):
        if not fname.startswith('log_') or not fname.endswith('.ndjson'):
            continue
        node_id = fname.replace('log_', '').replace('.ndjson', '')
        log_terms = []
        try:
            with open(os.path.join(STATE_DIR, fname), 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    term = entry.get('term')
                    if term is not None:
                        log_terms.append(term)
            logs[node_id] = log_terms
        except: # (json.JSONDecodeError, OSError):
            # Skip this file this cycle if it's currently being written
            continue
    return logs

def print_logs(logs, window_size: int):
    # Determine max log length
    max_len = max((len(log) for log in logs.values()), default=0)
    #window_size = 20

    # Calculate start index to slice logs, so we show last 20 entries of the longest log
    start_index = max(0, max_len - window_size)

    # Header with indexes
    header = '--i-' + ''.join(f'-{i:04}-' for i in range(start_index, max_len))
    print(header)

    for node_id in sorted(logs.keys()):
        row = f'[{node_id}]'

        log_slice = logs[node_id][start_index:] if len(logs[node_id]) > start_index else []
        # Pad with empty spaces if log shorter than start_index
        padding = window_size - len(log_slice)
        row += ''.join(f'-{term:04}-' for term in log_slice)
        row += '-' * 6 * padding  # 6 chars per entry like '-0000-'
        
        #row += ''.join(f'-{term:03}-' for term in logs[node_id])

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


def monitor_logs(window_size: int):
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            logs = load_logs()
            print_logs(logs, window_size)
            time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 2:
        print("Usage: python logs_visual.py <window_size>")
        exit(1)
    elif len(sys.argv) == 2:
        window_size = int(sys.argv[1])
    else:
        window_size = 10

    monitor_logs(window_size)
