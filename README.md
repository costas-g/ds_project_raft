# ds_project_raft

A Python implementation of the [Raft consensus algorithm](https://raft.github.io/) for distributed systems, developed as part of a university project.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Authors](#authors)
- [License](#license)
- [References](#references)

## Overview

This project implements the Raft consensus algorithm to manage a replicated log across a cluster of distributed nodes. It demonstrates core Raft features including leader election, log replication, and fault tolerance.

The codebase is designed for educational use and experimentation with distributed systems principles.

## Features

- **Leader Election:** Simulates leader election among Raft nodes.
- **Log Replication:** Supports append and replication of log entries.
- **Fault Tolerance:** Nodes handle failures and can rejoin the cluster.
- **Snapshotting:** Periodically creates snapshots of the replicated log to optimize memory usage and speed up recovery.
- **Batching:** Supports batching of client commands for efficient log replication and higher throughput.
- **Async Client:** Uses `asyncio` for modern, non-blocking client-server communication.
- **Threaded Nodes:** Each node runs in its own thread, using queues for inter-node communication.
- **Configurable Cluster:** Easily modify node configuration and network parameters.
- **Client Testing Command:** Provides tools for sending test commands to the cluster and measuring system response.


## Installation

1. **Clone the repository**
    ```bash
    git clone https://github.com/costas-g/ds_project_raft.git
    cd ds_project_raft
    ```

2. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

    Main dependencies:  
    - `raftos`  
    - Python standard library modules (`asyncio`, `threading`, etc.)

## Usage

1. **Configure the cluster**

    Edit `cluster_config.py` to define node addresses and ports.

2. **Start the Raft nodes**

    Open a terminal window for each node you wish to start.  
    Example for a 3-node cluster:

    ```bash
    python -m run.run_node n1
    ```

    Or you can run the ready 3 node cluster at once. 
    ```bash
    python -m run.run_cluster
    ```

3. **Run the client**

    The client connects to the cluster, detects the leader, and sends commands.

    ```bash
    python -m tests.client_load
    ```

    The client will automatically follow redirects from followers and retry if a node is offline.

## Project Structure

```plaintext
ds_project_raft-main/
├── logs/                    # Log files for each node (n1, n2, n3, ...)
│
├── raft/                    # Core Raft implementation
│   ├── messages/            # Message definitions for Raft protocol
│   │   ├── append_entries.py
│   │   ├── client_request.py
│   │   ├── install_snapshot.py
│   │   ├── init.py
│   │   ├── message.py
│   │   ├── request_vote.py
│   ├── command.py
│   ├── entry.py
│   ├── init.py
│   ├── raft_node.py
│   ├── raft_state.py
│   ├── snapshot.py
│   ├── state_machine.py
│
├── run/                     # Scripts to start and manage the cluster/client
│   ├── client.py
│   ├── cluster_config.py
│   ├── init.py
│   ├── run_client.py
│   ├── run_node.py
│
├── states/                  # Folder for storing node states as JSON
│   └── ...                  # JSON files
│
├── tests/                   # Test scripts
│   ├── client_load.py
│   ├── crashes.py
│   ├── demo.py
│   ├── demo2.py
│
├── util/                    # Utility scripts
│   ├── logs_visual.py
│   ├── tail_logs.py
│
└── README.md                # Project documentation




## Authors

- [costas-g](https://github.com/costas-g)
- [Panagiotis](https://github.com/yourusername)  <!-- Replace with your actual GitHub username if desired -->
- [Other contributors...]

## License

This project is licensed under the EKPA License.

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [`raftos` library](https://github.com/zhebrak/raftos)
- [In Search of an Understandable Consensus Algorithm (Raft paper)](https://raft.github.io/raft.pdf)

---

*For questions, bugs, or suggestions, please open an issue on this GitHub repository.*

