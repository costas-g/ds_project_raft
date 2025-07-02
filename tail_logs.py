import time
import heapq
import re
from pathlib import Path
import json
import run.cluster_config as config

LOG_DIR = Path("logs")
#CONFIG_FILE = Path("run/cluster_config_3.json")
#LOG_FILES = [LOG_DIR / f"n{i}.log" for i in range(1, 4)]
# Adjust the list if your logs differ

TIMESTAMP_PATTERN = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})')

def parse_timestamp(line):
    match = TIMESTAMP_PATTERN.match(line)
    if match:
        ts_str = match.group(1)
        # Convert to comparable format, here just keep string as it sorts lexicographically
        return ts_str
    return None

def tail_files(files):
    # Open files and seek to end
    file_handles = []
    for f in files:
        fh = open(f, 'r', encoding='utf-8')
        fh.seek(0, 2)  # go to EOF
        file_handles.append((f.name, fh)) # f is a Path, f.name is filename string

    heap = []
    # Buffer for each file's new lines
    buffers = {fname: [] for fname, _ in file_handles}

    try:
        while True:
            for fname, fh in file_handles:
                line = fh.readline()
                while line:
                    ts = parse_timestamp(line)
                    if ts:
                        # Push into heap by (timestamp, filename, line)
                        heapq.heappush(heap, (ts, fname, line.rstrip('\n')))
                    line = fh.readline()

            # Print lines in timestamp order, but only if heap has more than one item to avoid out-of-order
            while len(heap) > 1:
                ts, fname, line = heapq.heappop(heap)
                print(f"[{fname}] {line}")

            time.sleep(0.2)
    except KeyboardInterrupt:
        for _, fh in file_handles:
            fh.close()

if __name__ == "__main__":
    # Load config and build log file list dynamically
    # with open(CONFIG_FILE) as f:
    #     config = json.load(f)
    nodes = config.nodes
    LOG_FILES = [LOG_DIR / f"{node}.log" for node in nodes]
    tail_files(LOG_FILES)
