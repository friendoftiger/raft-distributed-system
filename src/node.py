import sys
import json
import logging
import requests
import random, time
import threading

from flask import Flask, request, jsonify
from collections import deque
from route import api_blueprint 

app = Flask(__name__)

# Status Constants
FOLLOWER = "Follower"
LEADER = "Leader"
CANDIDATE = "Candidate"

# Raft API Constants
RequestVote_API = "/request_vote"
AppendEntries_API = "/append_entries"

# Timeout
TIMEOUT_LOW = 1
TIMEOUT_HIGH = 1.5
Heartbeat_TIMEOUT = 0.3

# Node Class
class Node:
    def __init__(self, config_path, id):
        # Persistent states on all servers
        self.role = FOLLOWER
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        self.election_timeout = None    # Timer

        self.ip = None
        self.port = None
        self.id = None
        self.peers = []

        # Lock
        self.lock = threading.RLock()

        # Topics are stored as keys in a dictionary, messages are stored as a queue of a topic's value.
        self.topics = {}

        # Volatile states on all servers
        self.commitIndex = 0

        # Volatile states on leaders:
        self.nextIndex = {}
        self.matchIndex = {}

        self.load_config(config_path, id)
        self.reset_election_timeout()

        threading.Thread(target=self.election_monitor, daemon=False).start()

    # Helper to initialize a Node
    def load_config(self, config_path, id):
        with self.lock:
            with open(config_path, 'r') as f:
                config_info = json.load(f)
            addresses = config_info["addresses"]
            for i, address in enumerate(addresses):
                self.peers.append({
                    "peerId": i,
                    "ip": address["ip"], 
                    "port": address["port"]})
            self.ip = self.peers[int(id)]['ip']
            self.port = self.peers[int(id)]['port']
            self.id = id

    ########################################
    # Client API Methods
    ########################################
    def create_topic(self, topic):
        with self.lock:
            if topic in self.topics:
                return False
            else:
                self.topics[topic] = deque()               
                
                self.log.append({
                    'term': self.currentTerm,
                    'api': "put_topic",
                    'topic': topic
                })

                return True

    def get_topics(self):
        with self.lock:
            return list(self.topics.keys())
    
    def put_message(self, topic, message):
        with self.lock:
            if topic not in self.topics:
                return False
            else:
                self.topics[topic].append(message)

                self.log.append({
                    'term': self.currentTerm,
                    'api': "put_message",
                    'topic': topic,
                    'message': message
                })

                return True
    
    def get_message(self, topic):
        with self.lock:
            if topic not in self.topics:
                return False
            else:
                if len(self.topics[topic]) > 0:
                    get_message = self.topics[topic].popleft()

                    self.log.append({
                        'term': self.currentTerm,
                        'api': "get_message",
                        'topic': topic
                    })
                else:
                    return False

                return get_message
    
    def get_status(self):
        with self.lock:
            return self.role, self.currentTerm
    
    ########################################
    # Timer Methods
    ########################################
    def reset_election_timeout(self):
        self.election_timeout = time.time() + random.uniform(TIMEOUT_LOW, TIMEOUT_HIGH)
    
    def election_monitor(self):
        while True:
            with self.lock:
                timed_out = self.role != LEADER and time.time() > self.election_timeout

            if timed_out:
                threading.Thread(target=self.start_election, daemon=False).start()

            time.sleep(0.01)    # Short sleep to avoid tight looping
        
    ########################################
    # Raft Internal Methods
    ########################################
    def become_leader(self):
        with self.lock:
            self.role = LEADER
            leader_id = self.id

            # Initialize nextIndex and matchIndex for every follower
            for peer in self.peers:
                peerId = peer['peerId']
                self.nextIndex[peerId] = len(self.log)+1
                self.matchIndex[peerId] = 0

            threading.Thread(target=self.send_heartbeats, daemon=False).start()

    def become_follower(self, term):
        with self.lock:
            self.role = FOLLOWER
            if self.currentTerm < term:
                self.currentTerm = term
                self.votedFor = None    # Clear the voteFor 
            self.reset_election_timeout()

    def become_candidate(self):
        with self.lock:
            self.role = CANDIDATE
            self.currentTerm += 1
            self.votedFor = self.id     # Vote for myself
            self.reset_election_timeout()
        
    def log_entry(self, entry):
        with self.lock:
            if entry == {}:
                return
            
            with self.lock:
                if entry['api'] == "put_topic":
                    topic = entry['topic']
                    self.topics[topic] = deque()
                    self.log.append(entry)
                elif entry['api'] == "put_message":
                    topic = entry['topic']
                    message = entry['message']
                    self.topics[topic].append(message)
                    self.log.append(entry)
                elif entry['api'] == "get_message":
                    topic = entry['topic']
                    self.topics[topic].popleft()
                    self.log.append(entry)

    def log_replicate(self):
        with self.lock:
            if self.role != LEADER:
                return
            
            for peer in self.peers:
                if peer['peerId'] != self.id:
                    threading.Thread(target=self.append_entries, args=(peer, ), daemon=False)
                else:
                    self.matchIndex[peer['peerId']] = len(self.log)
            results = {}

            # Find majority of matchIndex among peers 
            for peer in self.peers:
                peer_matchIndex = self.matchIndex[peer['peerId']]
                results[peer_matchIndex] = results.get(peer_matchIndex, 0) + 1
            
            filtered_keys = [k for k, v in results.items() if v > len(self.peers) // 2]

            new_commit = max(filtered_keys) if filtered_keys else 0

            if new_commit > self.commitIndex:
                self.commitIndex = new_commit
            
        
    ########################################
    # Raft Messaging Methods
    ########################################
    def send_message(self, method, endpoint, ip, port, body=None): 
        url = f"http://{ip}:{port}{endpoint}"
        try:
            if method == "GET":
                response = requests.get(url, timeout=5)
            elif method == "PUT":
                response = requests.put(url, json=body, timeout=5)
            else:
                logging.error("send_message Error: Invalid method in message sending")
                return {"error": "Invalid method in message sending"}

            response.raise_for_status()  # Raise HTTPError for bad responses
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"send_message Error: {e}")
            return {"error": "Error in sending message"}
    
    def start_election(self):
        with self.lock:
            if len(self.peers) == 1:
                self.become_leader()    # Directly become leader in single-node system
                return
            
            self.become_candidate()
        
            total_votes = 1

            lastLogIndex = len(self.log)
            lastLogTerm = self.log[lastLogIndex-1]['term'] if len(self.log) > 0 else 0

            body = {
                'term': self.currentTerm,
                'candidateId': self.id,
                'lastLogIndex': lastLogIndex,
                'lastLogTerm': lastLogTerm
            }
        
            # Request votes from all other nodes
            for peer in self.peers:
                peerId = peer['peerId']
                ip = peer['ip']
                port = peer['port']
                if peerId == self.id:
                    continue    # Do not send voting request to leader itself
                try:
                    response = self.send_message("PUT", RequestVote_API, peer['ip'], peer['port'], body)
                    if 'error' not in response:
                        replyer_term = int(response["term"])
                        replyer_granted = response["granted"]
                        if replyer_term > self.currentTerm:
                            self.become_follower(replyer_term)   
                        elif replyer_granted:
                            total_votes += 1
                except requests.exceptions.RequestException as e:
                    pass  # Ignore any errors and continue

        if total_votes > len(self.peers)//2:
            self.become_leader()        # Win the election if received majority's votes

        return


    def send_heartbeats(self):
        """Send one round of heartbeats if leader."""
        while True:
            with self.lock:
                if self.role != LEADER:
                        break

                term = self.currentTerm
                leaderId = self.id
                leaderCommit = self.commitIndex

                for peer in self.peers:
                    peerId = peer['peerId']
                    prevIndex = max(self.nextIndex[peerId]-1, 0)
                    prevTerm = self.log[prevIndex - 1]['term'] if prevIndex > 0 else 0
                    if peerId != self.id:
                        body = {
                            'term': term,
                            'leaderId': leaderId,
                            'peerId': peerId,
                            'prevIndex': prevIndex,
                            'prevTerm': prevTerm,
                            'entry': {},
                            'leaderCommit': leaderCommit
                        }
                        try:
                            response = self.send_message('PUT', AppendEntries_API, peer['ip'], peer['port'], body)
                            if 'error' not in response:
                                replier_id = response['peerId']
                                reply_term = int(response["term"])
                                success = response["success"]
                                reply_matchIndex = int(response["matchIndex"])

                                # Become follower if got reply from higher term
                                if reply_term > self.currentTerm:
                                    self.become_follower(reply_term)
                                    return
                                
                                if success:
                                    # Update nextIndex and matchIndex for this peer
                                    self.nextIndex[peerId] = min(reply_matchIndex+1, len(self.log)+1)
                                    self.matchIndex[peerId] = reply_matchIndex
                                else:
                                    # If failing, decrement nextIndex, but don't go below 1
                                    self.nextIndex[peerId] = max(self.nextIndex[peerId] - 1, 1)
                                    self.matchIndex[peerId] = min(self.nextIndex[peerId], self.commitIndex)
                                
                                # Continue until log is fully replicates
                                if reply_matchIndex < len(self.log):
                                    self.append_entries(peer)

                        except requests.exceptions.RequestException as e:
                            pass  # Ignore error and continue
            time.sleep(Heartbeat_TIMEOUT)

    def append_entries(self, peer):
        with self.lock:
            if self.role != LEADER:
                return

            term = self.currentTerm
            leaderId = self.id
            leaderCommit = self.commitIndex

            peerId = peer['peerId']
            prevIndex = max(self.nextIndex[peerId]-1, 0)
            prevTerm = self.log[prevIndex - 1]['term'] if prevIndex > 0 else 0
            entry = self.log[self.nextIndex[peerId]-1] if self.nextIndex[peerId] <= len(self.log) else {}
           
            body = {
                'term': term,
                'leaderId': leaderId,
                'peerId': peerId,
                'prevIndex': prevIndex,
                'prevTerm': prevTerm,
                'entry': entry,
                'leaderCommit': leaderCommit
            }

            try:
                response = self.send_message("PUT", AppendEntries_API, peer['ip'], peer['port'], body)

                if 'error' not in response:
                    replier_id = response['peerId']
                    reply_term = int(response["term"])
                    success = response["success"]
                    reply_matchIndex = int(response["matchIndex"])

                    # Become follower if got reply from higher term
                    if reply_term > self.currentTerm:
                        self.become_follower(reply_term)
                        return
                    
                    if success:
                        # Update nextIndex and matchIndex for this peer
                        self.nextIndex[peerId] = min(reply_matchIndex+1, len(self.log)+1)
                        self.matchIndex[peerId] = reply_matchIndex
                    else:
                        # If failing, decrement nextIndex
                        self.nextIndex[peerId] = max(self.nextIndex[peerId] - 1, 1)
                        self.matchIndex[peerId] = min(self.nextIndex[peerId], self.commitIndex)
                    # Continue until log is fully replicates
                    if reply_matchIndex < len(self.log):
                        self.append_entries(peer)

            except requests.exceptions.RequestException as e:
                pass  # Ignore error and continue
                    

def create_app(config_path, id):
    app = Flask(__name__)    # Create the Flask app
    node = Node(config_path, id)    # Create the node instance
    app.config['node'] = node   # Store the node in Flask's config
    app.register_blueprint(api_blueprint)   # Register the blueprint
    return app

if __name__ == '__main__':
    """ USAGE: python src/node.py config.json <id>"""
    config_path = sys.argv[1]
    id = int(sys.argv[2])
    app = create_app(config_path, id)
    node = app.config['node']

    def run_flask():
        try:
            app.run(host=node.ip, port=node.port, debug=False, use_reloader=False)
        except Exception as e:
            sys.exit(1)

    # Run Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=False)
    flask_thread.start()

    # Keep the main thread running
    try:
        while True:
            time.sleep(0.01)
    except KeyboardInterrupt:
        sys.exit(0)