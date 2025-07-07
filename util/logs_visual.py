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

def get_snapshot_index(node_id):
    path = os.path.join(STATE_DIR, f'snapshot_{node_id}.json')
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f).get('last_included_index', -1)
    return -1  # if no snapshot, treat as empty

def print_logs(logs, window_size: int, log_start_index: int = 0):
    # Filter out nodes with empty or missing logs
    active_logs = {nid: log for nid, log in logs.items() if log}

    if not active_logs:
        print("No active logs to display.")
        return

    # Find max global index only among active logs
    max_global_index = -1
    for node_id, log in active_logs.items():
        snapshot_index = get_snapshot_index(node_id)
        log_start = snapshot_index + 1
        global_end = log_start + len(log) - 1
        if global_end > max_global_index:
            max_global_index = global_end

    # Calculate start index for window
    start_index = max(0, max_global_index - window_size + 1)

    # Print header
    header = f'{(start_index//1000):02}k+' + ''.join(f'-{(i%1000):03}-' for i in range(start_index, start_index + window_size))
    print(header)

    selected_log_start_index = get_log_start_index_of_log_with_highest_last_global_index()

    # Print only nodes with logs (skip offline)
    for node_id in sorted(active_logs.keys()):
        row = f'[{node_id}]'
        snapshot_index = get_snapshot_index(node_id)
        log_start = snapshot_index + 1
        log = active_logs[node_id]

        for i in range(start_index, start_index + window_size):
            local_index = i - log_start
            if 0 <= local_index < len(log):
                row += f'-{log[local_index]:03}-'
            else:
                row += '-----'

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

def get_log_start_index_of_log_with_highest_last_global_index():
    max_last_global_index = -1
    selected_log_start_index = None

    for filename in os.listdir(STATE_DIR):
        if filename.startswith('log_') and filename.endswith('.ndjson'):
            node_id = filename[4:-7]
            log_path = os.path.join(STATE_DIR, filename)
            snapshot_path = os.path.join(STATE_DIR, f'snapshot_{node_id}.json')

            # Load last_included_index
            last_included_index = -1
            if os.path.exists(snapshot_path):
                with open(snapshot_path, 'r') as f:
                    data = json.load(f)
                    last_included_index = data.get('last_included_index', -1)

            log_start_index = last_included_index + 1

            # Count log entries
            with open(log_path, 'r') as f:
                log_len = sum(1 for _ in f)

            last_global_index = log_start_index + log_len - 1

            if last_global_index > max_last_global_index:
                max_last_global_index = last_global_index
                selected_log_start_index = log_start_index

    return selected_log_start_index

def monitor_logs(window_size: int):
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            logs = load_logs()
            log_start_index = get_log_start_index_of_log_with_highest_last_global_index()
            print_logs(logs, window_size, log_start_index)
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
