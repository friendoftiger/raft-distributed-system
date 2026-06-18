import sys
import requests
import time
import logging
import json

# Method constants
HTTP_GET = "GET"
HTTP_PUT = "PUT"

# Endpoints constants
ENDPOINT_TOPIC = "/topic"
ENDPOINT_MESSAGE = "/message"
ENDPOINT_STATUS = "/status"

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='[CLIENT] %(message)s')



def load_config(config_path):
    peers = []
    with open(config_path, 'r') as f:
        config_info = json.load(f)
    addresses = config_info["addresses"]
    for i, address in enumerate(addresses):
        peers.append({
            "peerId": i,
            "ip": address["ip"], 
            "port": address["port"]})
    return peers

def find_leader(peers):
    for peer in peers:
        ip = peer['ip']
        port = peer['port']
        url = f"http://{ip}:{port}/status"
        logging.info(f"Looking role for node {peer['peerId']} on {ip}:{port}")
        response = send_request('GET', url)
        if response['role'] == 'Leader':
            return peer
        else:
            continue
    return None



def send_request(method, url, body=None):
    try:
        if method == "GET":
            response = requests.get(url, timeout=5).json()
            logging.info(f"Received response: {response}")
            return response
        elif method == "PUT":
            response = requests.put(url, json=body, timeout=5).json()
            logging.info(f"Received response: {response}")
            return response
        else:
            logging.error("Invalid Method in command")
            sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

def route():
    action = sys.argv[2]
    config_path = sys.argv[1]
    peers = load_config(config_path)
    leader = find_leader(peers)
    if leader:
        ip = leader['ip']
        port = leader['port']
        base_url = f"http://{ip}:{port}"
    else:
        logging.error("Can not find leader")
        return
    
    if action == "put_topic":
        """ Usage: python src/client.py config.json put_topic <topic> """
        topic = sys.argv[3]
        body = {'topic': topic}
        send_request(HTTP_PUT, f"{base_url}{ENDPOINT_TOPIC}", body)
        
    elif action == "get_topic":
        """ Usage: python src/client.py config.json get_topic """
        send_request(HTTP_GET, f"{base_url}{ENDPOINT_TOPIC}")

    elif action == "put_message":
        """ Usage: python src/client.py config.json put_message <message> <topic> """
        message = sys.argv[3]
        topic = sys.argv[4]
        body = {
            'topic': topic,
            'message': message
        }
        send_request(HTTP_PUT, f"{base_url}{ENDPOINT_MESSAGE}", body)

    elif action == "get_message":
        """ Usage: python src/client.py config.json get_message <topic> """
        topic = sys.argv[3]
        send_request(HTTP_GET, f"{base_url}{ENDPOINT_MESSAGE}/{topic}")

    elif action == "get_status":
        """ Usage: python src/client.py config.json get_status <node_id> """
        node_id = sys.argv[3]
        send_request(HTTP_GET, f"{base_url}{ENDPOINT_STATUS}")
    else:
        logging.error("Invalid Action in command")
        sys.exit(1)

if __name__ == "__main__":
    route()
