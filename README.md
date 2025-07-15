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

- **Leader Election:** Automatic election of a cluster leader.
- **Log Replication:** Consistent log entry replication across nodes.
- **Fault Tolerance:** Nodes handle crashes and can recover/rejoin the cluster.
- **Async Client:** Modern `asyncio`-based TCP client for interacting with the Raft cluster.
- **Threaded Node Simulation:** Each node runs in its own thread and communicates via Python queues.
- **Customizable Cluster:** Easily configurable node settings.

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
    python node.py --id 1
    python node.py --id 2
    python node.py --id 3
    ```

3. **Run the client**

    The client connects to the cluster, detects the leader, and sends commands.

    ```bash
    python client.py
    ```

    The client will automatically follow redirects from followers and retry if a node is offline.

## Project Structure

ds_project_raft/
│
├── node.py # Main Raft node logic
├── client.py # Asyncio-based TCP client
├── cluster_config.py # Cluster node configuration
├── raft/ # Raft protocol logic (utilities, message handling)
├── requirements.txt # Python package requirements
└── README.md # Project documentation


## Authors

- [costas-g](https://github.com/costas-g)
- [Panagiotis](https://github.com/yourusername)  <!-- Replace with your actual GitHub username if desired -->
- [Other contributors...]

## License

This project is licensed under the MIT License.

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [`raftos` library](https://github.com/zhebrak/raftos)
- [In Search of an Understandable Consensus Algorithm (Raft paper)](https://raft.github.io/raft.pdf)

---

*For questions, bugs, or suggestions, please open an issue on this GitHub repository.*

