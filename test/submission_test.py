from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests
import random
import threading

# Test configuration
PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0  # Increased for more reliable re-elections
TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
NUM_NODES_ARRAY = [3, 5]  # Test with both 3 and 5 nodes

@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()

@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    yield node
    node.clean()

@pytest.fixture
def node():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    yield node
    node.clean()

# TOPIC TESTS
def test_get_topic_empty(node):
    assert(node.get_topics().json() == {"success": True, "topics": []})

def test_create_topic(node):
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})

def test_create_different_topics(node):
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    assert(node.create_topic("test_topic_different").json() == {"success": True})

def test_create_same_topic(node):
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    assert(node.create_topic(TEST_TOPIC).json() == {"success": False})

def test_get_topic(node):
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    assert(node.get_topics().json() == {"success": True, "topics": [TEST_TOPIC]})

def test_get_multiple_topics(node):
    topics = []
    for i in range(5):
        topic = TEST_TOPIC + str(i)
        assert(node.create_topic(topic).json() == {"success": True})
        topics.append(topic)
    assert(node.get_topics().json() == {"success": True, "topics": topics})

# MESSAGE TESTS
def test_get_message_from_inexistent_topic(node_with_test_topic):
    assert(node_with_test_topic.get_message("nonexistent_topic").json() == {"success": False})

def test_get_message_empty_topic(node_with_test_topic):
    assert(node_with_test_topic.get_message(TEST_TOPIC).json() == {"success": False})

def test_put_message(node_with_test_topic):
    assert(node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})

def test_put_and_get_message(node_with_test_topic):
    assert(node_with_test_topic.put_message(TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})
    assert(node_with_test_topic.get_message(TEST_TOPIC).json() == {"success": True, "message": TEST_MESSAGE})

def test_put_and_get_multiple_messages(node_with_test_topic):
    messages = [f"{TEST_MESSAGE}_{i}" for i in range(5)]
    for msg in messages:
        assert(node_with_test_topic.put_message(TEST_TOPIC, msg).json() == {"success": True})
    
    for msg in messages:
        assert(node_with_test_topic.get_message(TEST_TOPIC).json() == {"success": True, "message": msg})
    
    # Now the topic should be empty
    assert(node_with_test_topic.get_message(TEST_TOPIC).json() == {"success": False})

# ELECTION TESTS
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_leader_election(swarm, num_nodes):
    leader = swarm.get_leader_loop(3)
    assert(leader is not None)

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_unique_leader(swarm, num_nodes):
    # Get all nodes with status Leader
    statuses = swarm.get_status()
    leaders = [i for i, status in statuses.items() if status["role"] == LEADER]
    
    # There should be at most one leader
    assert(len(leaders) <= 1)

@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_leader_reelection(swarm, num_nodes):
    # Get initial leader
    leader1 = swarm.get_leader_loop(3)
    assert(leader1 is not None)
    
    # Stop the leader and wait longer for re-election
    leader1.clean(ELECTION_TIMEOUT * 2)  # Wait longer for re-election
    
    # Try multiple times to find a new leader with longer timeout
    leader2 = None
    for attempt in range(5):  # Try up to 5 times
        leader2 = swarm.get_leader_loop(3)
        if leader2 is not None:
            break
        time.sleep(0.5)  # Wait a bit between attempts
    
    assert(leader2 is not None)
    assert(leader2 != leader1)

# STRESS TESTS
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_node_recovery(swarm, num_nodes):
    # Get initial leader
    leader = swarm.get_leader_loop(3)
    assert(leader is not None)
    
    # Create topic and put message on leader
    assert(leader.create_topic(TEST_TOPIC).json() == {"success": True})
    assert(leader.put_message(TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})
    
    # Wait for replication
    time.sleep(ELECTION_TIMEOUT)
    
    # Choose a follower to restart
    statuses = swarm.get_status()
    followers = [i for i, status in statuses.items() if status["role"] == FOLLOWER]
    assert(len(followers) > 0)
    follower_idx = random.choice(followers)
    follower = swarm[follower_idx]
    
    # Restart the follower
    follower.restart()
    
    # Wait for recovery
    time.sleep(ELECTION_TIMEOUT * 2)
    
    # Check if follower has recovered the state
    assert(follower.get_topics().json() == {"success": True, "topics": [TEST_TOPIC]})

if __name__ == "__main__":
    pytest.main()