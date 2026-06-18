# Raft Distributed Message Queue

## Author: Pengyue Zhao

A Python implementation of a replicated message queue backed by the Raft consensus algorithm. The project exposes a REST API for creating topics, publishing messages, consuming messages, and checking node status.

## Reports

- [Technical Report](technical-report.md): Explains the Raft data structures, election flow, log replication, synchronization approach, and known limitations.
- [Testing Report](testing-report.md): Summarizes pytest coverage for message queue behavior, leader election, replication, and fault-tolerance scenarios.

## Features

- Leader election across a local cluster
- Heartbeat-based leader maintenance
- Log replication from leader to followers
- Topic creation and FIFO message consumption
- Pytest integration tests for queue operations, elections, and replication

## Requirements

- Python 3.9+
- pip

## Setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Tests

Run the full test suite from the repository root:

```bash
python -m pytest
```

The tests create a temporary `config.json`, start local node processes, send HTTP requests to those nodes, and clean up the processes after each test.

## Run A Local Cluster

Create a local config from the example file:

```bash
cp config.example.json config.json
```

Start each node in a separate terminal:

```bash
python src/node.py config.json 0
python src/node.py config.json 1
python src/node.py config.json 2
python src/node.py config.json 3
python src/node.py config.json 4
```

Wait a couple of seconds for leader election, then use the client:

```bash
python src/client.py config.json get_topic
python src/client.py config.json put_topic demo
python src/client.py config.json put_message "hello raft" demo
python src/client.py config.json get_message demo
```

You can also check node status directly:

```bash
curl http://127.0.0.1:51030/status
curl http://127.0.0.1:51032/status
curl http://127.0.0.1:51034/status
```

Exactly one healthy node should report `"role": "Leader"` after election settles.

## API

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/status` | Return node role and term |
| `PUT` | `/topic` | Create a topic with JSON body `{"topic": "demo"}` |
| `GET` | `/topic` | List topics on the node |
| `PUT` | `/message` | Publish with JSON body `{"topic": "demo", "message": "hello"}` |
| `GET` | `/message/<topic>` | Consume the next message from a topic |

Client writes should be sent to the leader. The included `src/client.py` discovers the leader before issuing commands.

## Repository Layout

```text
src/
  client.py        CLI helper that finds the leader and calls the REST API
  node.py          Raft node process and cluster behavior
  route.py         Flask route definitions
test/
  *_test.py        Pytest integration tests
config.example.json
technical-report.md
testing-report.md
```
