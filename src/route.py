# routes.py

from flask import Blueprint, request, jsonify, current_app

# Create a Blueprint instead of using `app` directly
api_blueprint = Blueprint('api_blueprint', __name__)

########################################
# Client API
########################################
@api_blueprint.route('/topic', methods=['PUT'])
def put_topic_api():
    node = current_app.config['node'] 
    data = request.get_json()

    if not data or 'topic' not in data:
        return jsonify({'success': False}), 400
    
    topic = data['topic']
    success = False

    with node.lock:
        if node.role != "Leader":
            return jsonify({'success': False}), 400
        
        log_success = node.create_topic(topic)   
        node.log_replicate()

    return jsonify({'success': log_success}), (200 if log_success else 500)


@api_blueprint.route('/topic', methods=['GET'])
def get_topic_api():
    node = current_app.config['node']
    topic_list = node.get_topics()

    return jsonify({'success': True, 'topics': topic_list}), 200


@api_blueprint.route('/message', methods=['PUT'])
def put_message_api():
    node = current_app.config['node']

    data = request.get_json()

    if not data or 'topic' not in data or 'message' not in data:
        return jsonify({'success': False}), 400
    
    topic = data['topic']
    message = data['message']
    
    with node.lock:
        if node.role != "Leader":
            return jsonify({'success': False}), 400
        
        log_success = node.put_message(topic, message)
        node.log_replicate()

    return jsonify({'success': log_success}), (200 if log_success else 500)


@api_blueprint.route('/message/<topic>', methods=['GET'])
def get_message_api(topic):
    node = current_app.config['node']

    with node.lock:
        if node.role != "Leader":
            return jsonify({'success': False}), 400
        
        message = node.get_message(topic)
        node.log_replicate()

        if message != False:
            return jsonify({'success': True, 'message': message}), 200
        else:
            return jsonify({'success': False}), 500

@api_blueprint.route('/status', methods=['GET'])
def get_status_api():
    node = current_app.config['node']
    with node.lock:
        role, term = node.get_status()

    return jsonify({'role': role, 'term': term}), 200

########################################
# Raft API
########################################
@api_blueprint.route('/request_vote', methods=['PUT'])
def request_vote_api():
    node = current_app.config['node']
    
    data = request.get_json()
    if not data or 'term' not in data or 'candidateId' not in data:
        return jsonify({"error": "Invalid Voting Request"}), 400

    candidate_term = int(data['term'])
    candidate_id = data['candidateId']
    candidate_lastLogIndex = data['lastLogIndex']
    candidate_lastLogTerm = data['lastLogTerm']

    with node.lock:
        my_term = node.currentTerm
        my_lastLogIndex = len(node.log)
        my_lastLogTerm = node.log[my_lastLogIndex-1]['term'] if len(node.log) > 0 else 0

        granted = False
 
        # 1. Compare my_term to candidate_term, reject if my_term is larger
        # 2. Compare my_lastLogIndex to candidate_lastLogIndex, reject if my_lastLogIndex is larger
        # 3. Compare my_lastLogTerm to candidate_lastLogTerm, reject if not matches
        # 4. Check votedFor, grant vote if not already voted
        if candidate_term > my_term:
            node.become_follower(candidate_term)
            if candidate_lastLogIndex >= my_lastLogIndex:
                if candidate_lastLogTerm == my_lastLogTerm:
                    if node.votedFor == None:
                        granted = True
                        node.votedFor = candidate_id

        response = {
            'term': node.currentTerm,
            'granted': granted
        }

        return jsonify(response), 200
    
@api_blueprint.route('/append_entries', methods=['PUT'])
def append_entries_api():
    node = current_app.config['node']

    data = request.get_json()

    if not data or 'term' not in data or 'prevIndex' not in data or 'prevTerm' not in data or 'entry' not in data or 'leaderCommit' not in data:
        return jsonify({"error": "Invalid AppendEntries Request"}), 400
    
    leader_term = int(data['term'])
    leader_id = data['leaderId']
    prevIndex = int(data['prevIndex'])
    prevTerm = int(data['prevTerm'])
    entry = data['entry']
    leaderCommit = int(data['leaderCommit'])

    success = False

    with node.lock:
        reply_matchIndex = 0
        reply_term = node.currentTerm
        peerId = node.id

        if reply_term <= leader_term:
            node.become_follower(leader_term)
            reply_term = node.currentTerm

            if prevIndex <= len(node.log):
                if len(node.log) == 0:
                    success = True
                    reply_matchIndex = prevIndex
                    node.log = node.log[:prevIndex]     # Deleting following conflict entries
                    node.log_entry(entry)   # Log this entry to the node
                    if entry != {}:
                        reply_matchIndex += 1
                    
                    if leaderCommit > node.commitIndex:
                        node.commitIndex = min(leaderCommit, len(node.log))     # Update node's commitIndex
                elif prevTerm == node.log[prevIndex-1]['term']:
                    success = True
                    reply_matchIndex = prevIndex
                    node.log = node.log[:prevIndex]     # Deleting following conflict entries
                    node.log_entry(entry)       # Log this entry to the node
                    if entry != {}:
                        reply_matchIndex += 1
                    if leaderCommit > node.commitIndex:
                        node.commitIndex = min(leaderCommit, len(node.log))     # Update node's commitIndex
           
        response = {
            'peerId': peerId,
            'term': reply_term,
            'success': success,
            'matchIndex': reply_matchIndex
        }
        return jsonify(response), 200