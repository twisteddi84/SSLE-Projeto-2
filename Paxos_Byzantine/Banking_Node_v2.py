import atexit
import sqlite3
import json
import socket
import sys
import threading
import time

import requests

from collections import defaultdict

#node 1 = 10.151.101.173
#node 2 = 10.151.101.45
#node 3 = 10.151.101.253
#node 4 = 10.151.101.10
#node 5 = 10.151.101.83

node_ip = "127.0.0.1"
#registry_ip = 10.151.101.221
registry_ip = "127.0.0.1"

active_nodes = {}
max_proposal = 0

class BankingService:
    def __init__(self, db_name="banking.db"):
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        """Create the accounts table if it doesn't exist."""
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                balance REAL NOT NULL DEFAULT 0.0
            )
            """)

    def create_account(self, name, initial_balance=0.0):
        """Create a new account."""
        with self.conn:
            self.conn.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", (name, initial_balance))
            print(f"Account created for {name} with initial balance {initial_balance}.")

    def get_balance(self, name):
        """Get the balance of an account."""
        cursor = self.conn.execute("SELECT balance FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            print(f"No account found for {name}.")
            return None

    def deposit(self, name, amount):
        """Deposit money into an account."""
        balance = self.get_balance(name)
        if balance is not None:
            new_balance = balance + amount
            with self.conn:
                self.conn.execute("UPDATE accounts SET balance = ? WHERE name = ?", (new_balance, name))
                print(f"Deposited {amount} into {name}'s account. New balance: {new_balance}.")

    def withdraw(self, name, amount):
        """Withdraw money from an account."""
        balance = self.get_balance(name)
        if balance is not None:
            if balance >= amount:
                new_balance = balance - amount
                with self.conn:
                    self.conn.execute("UPDATE accounts SET balance = ? WHERE name = ?", (new_balance, name))
                    print(f"Withdrew {amount} from {name}'s account. New balance: {new_balance}.")
            else:
                print(f"Insufficient funds in {name}'s account. Available balance: {balance}.")

    def close(self):
        """Close the database connection."""
        self.conn.close()

def menu(node_id):
    db_name = f"banking_node_{node_id}.db"  # Unique DB for each node
    banking_service = BankingService(db_name=db_name)  # Initialize once for this node
    print(f"Node {node_id} is running with database '{db_name}'")

    while True:
        print(f"\n--- Banking Service Menu for Node {node_id} ---")
        print("1. Create Account")
        print("2. Deposit Money")
        print("3. Withdraw Money")
        print("4. Check Balance")
        print("5. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            name = input("Enter account holder's name: ")
            initial_balance = float(input("Enter initial balance: "))
            action = {"action": "create_account", "name": name, "initial_balance": initial_balance}
            if send_prepare_message(node_id):
                #send propose message
                send_propose_message(node_id, action)

        elif choice == "2":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to deposit: "))
            action = {"action": "deposit", "name": name, "amount": amount}
            if send_prepare_message(node_id):
                #send propose message
                send_propose_message(node_id, action)


        elif choice == "3":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to withdraw: "))
            action = {"action": "withdraw", "name": name, "amount": amount}
            if send_prepare_message(node_id):
                #send propose message
                send_propose_message(node_id, action)

        elif choice == "4":
            name = input("Enter account holder's name: ")
            balance = banking_service.get_balance(name)
            if balance is not None:
                print(f"{name}'s current balance: {balance}")

        elif choice == "5":
            print(f"Exiting Banking Service for Node {node_id}. Goodbye!")
            banking_service.close()
            break

        else:
            print("Invalid choice. Please try again.")

def send_prepare_message(node_id):
    """
    Sends a Prepare message to all other active nodes in the cluster using sockets.
    """
    global active_nodes, max_proposal
    max_proposal += 1  # Increment global proposal number
    prepare_message = {"type": "prepare", "proposal_number": max_proposal}
    promises_received = 0
    majority = ((len(active_nodes) - 1) // 2) + 1  # Majority threshold

    print(f"Node {node_id} is sending Prepare message with proposal number {max_proposal}...")

    for other_node_id, node_info in active_nodes.items():
        if str(other_node_id) == str(node_id):
            continue  # Skip sending to itself
        
        try:
            # Extract host and port from the node's URL
            host = node_info['url'].split(":")[1].replace("/", "")
            port = node_info['url'].split(":")[2]
            port = int(port)

            # Connect to the other node
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))

                # Send the prepare message
                s.sendall(json.dumps(prepare_message).encode())

                # Receive the response
                response_data = s.recv(1024).decode()
                response = json.loads(response_data)

                # Check the response
                if response.get("status") == "promise":
                    promises_received += 1
                    print(f"Node {other_node_id} responded with Promise.")
                else:
                    print(f"Node {other_node_id} rejected Prepare: {response}")

        except (socket.error, json.JSONDecodeError) as e:
            print(f"Node {other_node_id} did not respond or failed to process the message: {e}")

    print(f"Promises received: {promises_received}/{len(active_nodes) - 1} (Majority needed: {majority})")
    return promises_received >= majority

def send_propose_message(node_id, action):
    """
    Sends a Propose message to all acceptors with the value to be accepted.
    """
    global active_nodes, max_proposal
    propose_message = {
        "type": "propose",
        "proposal_number": max_proposal,
        "action": action,
        "proposer_id": node_id

    }

    print(f"Node {node_id} is sending Propose message with proposal number {max_proposal} and action {action}...")

    for other_node_id, node_info in active_nodes.items():
        if str(other_node_id) == str(node_id):
            continue  # Skip sending to itself
        
        try:
            # Extract host and port from the node's URL
            host = node_info['url'].split(":")[1].replace("/", "")
            port = node_info['url'].split(":")[2]
            port = int(port)

            # Connect to the other node
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))

                # Send the propose message
                s.sendall(json.dumps(propose_message).encode())



        except (socket.error, json.JSONDecodeError) as e:
            print(f"Node {node_id} failed to send Propose message to {other_node_id}: {e}")

def broadcast_verification_message(proposal_number, status, node_id, action, proposer_id):
    """
    Function to send a verification message to all other nodes except the proposer.
    """
    verification_message = {
        "type": "verify",
        "proposal_number": proposal_number,
        "status": status,
        "action": action,
        "node_id": node_id,
        "proposer_id": proposer_id
    }

    for other_node_id, node_info in active_nodes.items():
        # Skip the proposer node
        if str(other_node_id) == str(proposer_id):
            print(f"Skipping proposer Node {proposer_id}.")
            continue

        try:
            # Extract host and port from the node's URL
            host = node_info['url'].split(":")[1].replace("/", "")
            port = 6000  # Adjust this to the correct port if needed
            port = int(port)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(json.dumps(verification_message).encode())
        except (socket.error, json.JSONDecodeError) as e:
            print(f"Node {node_id} failed to send verification message to {other_node_id}: {e}")

def stop_listening(stop_flag, proposal_number):
    """
    Function to stop listening after the time limit (10 seconds).
    Sets a stop flag to True when the timer expires.
    """
    time.sleep(10)
    print("Time limit reached. Stopping the listener.")
    stop_flag[proposal_number] = True

def listen_for_broadcasts(node_id):
    """
    Function to listen for incoming broadcast verification messages from other nodes.
    This will handle the verification of proposals from other nodes and check for malicious nodes using BFT formula.
    """
    global active_nodes
    total_nodes = len(active_nodes)

    f = (total_nodes - 1) // 3  # Maximum number of malicious nodes
    threshold = 2 * f + 1  # Threshold for BFT consensus

    # Track the responses for each proposal number
    proposal_responses = defaultdict(list)  # proposal_number -> list of {node_id, status}

    host = "0.0.0.0"
    port = 6000  # Use a different port for broadcast communication
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Node {node_id} listening for broadcasts on port {port}...")

    # Set a timeout for accepting connections (non-blocking mode)
    server_socket.settimeout(1.0)  # 1 second timeout

    # Flags to stop listening after time expires {proposal_number: stop_flag}
    stop_flag = {}


    while True:  # Continue listening until time expires
        try:
            # Try to accept a connection (this will not block for more than the set timeout)
            client_socket, addr = server_socket.accept()  # Accept incoming connection
            print(f"Received broadcast message from {addr}")

            # Receive the message from the client
            message_data = client_socket.recv(1024).decode()
            try:
                message = json.loads(message_data)
                print(f"Received broadcast message: {message}")

                if message.get("type") == "verify":
                    proposal_number = message["proposal_number"]
                    node_id_received = message["node_id"]
                    status = message["status"]
                    action = message["action"]
                    proposer_id = message["proposer_id"]
                    print(f"Node {node_id} received broadcast verification for proposal {proposal_number}")

                    # Check if this is a new proposal number that we haven't started a timer for yet
                    if proposal_number not in stop_flag:
                        stop_flag[proposal_number] = False
                        # Start a timer to stop listening after 10 seconds
                        timer = threading.Thread(target=stop_listening, args=(stop_flag, proposal_number))
                        timer.start()

                    # Add the response to the list of responses for this proposal number
                    proposal_responses[proposal_number].append({
                        "node_id": node_id_received,
                        "status": status,
                        "action": action,
                        "proposer_id": proposer_id
                    })

                else:
                    print(f"Received unexpected message type: {message.get('type')}")

            except json.JSONDecodeError:
                print(f"Failed to decode broadcast message from {addr}. Ignoring.")
            except Exception as e:
                print(f"Error processing broadcast message from {addr}: {e}")

            finally:
                client_socket.close()

        except socket.timeout:
            # Timeout reached, check if any proposals have expired
            expired_proposals = []  # Store expired proposals to delete after iteration
            for proposal_number, stop_flag_value in stop_flag.items():
                if stop_flag_value == False:
                    continue
                else:
                    verify_proposal(proposal_number,active_nodes,proposal_responses)
                    expired_proposals.append(proposal_number)

            # Remove expired proposals from stop_flag after the iteration
            for proposal_number in expired_proposals:
                del stop_flag[proposal_number]
        except Exception as e:
            print(f"Error accepting connection: {e}")
            continue


def get_reputation(node_id):
    """
    Get the reputation of a node from the active nodes dictionary.
    """
    global active_nodes
    return active_nodes[str(node_id)]['reputation'] if str(node_id) in active_nodes else 0

def verify_proposal(proposal_number, active_nodes, proposal_responses):
    """
    Function to verify the proposal responses and check for BFT consensus.
    """
    global max_proposal

    #Remove nodes under 50 reputation
    valid_responses = [
        response for response in proposal_responses[proposal_number]
        if "node_id" in response and get_reputation(response["node_id"]) >= 50
    ]

    print("Print total nodes: ", valid_responses)

    total_nodes = len(valid_responses) # Exclude the proposer node
    f = (total_nodes - 1) // 3  # Maximum number of malicious nodes
    threshold = 2 * f + 1  # Threshold for BFT consensus
    malicious_nodes = []

    print(f"Verifying proposal {proposal_number} responses...")

    if total_nodes < 3:
        print(f"Insufficient nodes for BFT consensus. Minimum 3 nodes required, but {total_nodes} found.")
        return

    # Get the responses for this proposal number
    responses = proposal_responses[proposal_number]

    # Count the number of approvals and rejections
    approvals = 0
    rejections = 0

    for response in responses:
        if response["status"] == "approved":
            approvals += 1
        elif response["status"] == "rejected":
            rejections += 1

    print(f"Approvals: {approvals}, Rejections: {rejections}")

    #Verify the majority response action and add the malicious ones to the lis


    # Check if the proposal is approved by the threshold
    if approvals >= threshold:
        print(f"Proposal {proposal_number} is approved by the threshold of {threshold}.")

        action_count = defaultdict(int)

        for response in responses:
            if response["status"] == "rejected":
                continue
            action = response["action"]

            # Convert the action dictionary to a tuple of sorted key-value pairs for hashing
            action_key = tuple(sorted(action.items()))

            action_count[action_key] += 1

        # Determine the majority action
        majority_action_key = max(action_count, key=action_count.get)

        # Convert the majority action key back to a dictionary for comparison
        majority_action = dict(majority_action_key)

        # Identify malicious nodes
        malicious_nodes = [
            str(response["node_id"]) for response in responses
            if "action" not in response or dict(sorted(response["action"].items())) != majority_action
        ]

        #Append reject nodes to malicious nodes
        for response in responses:
            if response["status"] == "rejected":
                malicious_nodes.append(str(response["node_id"]))

        print(f"Majority action: {majority_action}")
        print(f"Malicious nodes: {malicious_nodes}")

        send_learn_message(response["proposer_id"], proposal_number, majority_action, node_id, malicious_nodes)

        # Perform the action locally
        perform_action(majority_action, BankingService(db_name=f"banking_node_{node_id}.db"))

        # Increase reputation for non-malicious nodes
        for node in active_nodes:
            if node not in malicious_nodes:
                print(f"Reputation increased for Node {node}.")
                increase_reputation(node)
            else:
                print(f"Reputation decreased for Node {node}.")
                decrease_reputation(node)
        print("Final list of active nodes: ", active_nodes)
    else:
        print(f"Proposal {proposal_number} is rejected by the threshold of {threshold}.")
        # Send 'rejected' message to all nodes
        # broadcast_verification_message(proposal_number, "rejected", node_id)

def send_learn_message(proposer_id, proposal_number, action, node_id, malicious_nodes):
    """
    Sends a 'learn' message to the proposer node with the result of the proposal.
    
    """

    global active_nodes

    learn_message = {
        "type": "learn",
        "proposal_number": proposal_number,
        "action": action,
        "node_id": node_id,
        "malicious_nodes": malicious_nodes
    }

    proposer_info = active_nodes.get(str(proposer_id))
    if not proposer_info:
        print(f"Proposer {proposer_id} not found in active nodes.")
        return

    try:
        host = proposer_info['url'].split(":")[1].replace("/", "")
        port = 7000
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(learn_message).encode())
            print(f"Learn message sent to proposer {proposer_id}.")
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Failed to send learn message to proposer {proposer_id}: {e}")

def listen_for_learn_messages(node_id):
    """
    Function to listen for incoming 'learn' messages from other nodes.
    This will update the local database with the learned values.
    """
    host = "0.0.0.0"
    port = 7000  # Use a different port for broadcast communication
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Node {node_id} listening for Learning on port {port}...")

    # Set a timeout for accepting connections (non-blocking mode)
    server_socket.settimeout(1.0)  # 1 second timeout

    # Flags to stop listening after time expires {proposal_number: stop_flag}
    stop_flag = {}

    proposal_responses = defaultdict(list)  # proposal_number -> list of {node_id, status}

    while True:  # Continue listening until time expires
        try:
            # Try to accept a connection (this will not block for more than the set timeout)
            client_socket, addr = server_socket.accept()  # Accept incoming connection
            print(f"Received Learn message from {addr}")

            # Receive the message from the client
            message_data = client_socket.recv(1024).decode()
            try:
                message = json.loads(message_data)
                print(f"Received Learn message: {message}")

                if message.get("type") == "learn":
                    proposal_number = message["proposal_number"]
                    node_id_received = message["node_id"]
                    action = message["action"]
                    malicious_nodes = message["malicious_nodes"]

                    # Check if this is a new proposal number that we haven't started a timer for yet
                    if proposal_number not in stop_flag:
                        stop_flag[proposal_number] = False
                        # Start a timer to stop listening after 10 seconds
                        timer = threading.Thread(target=stop_listening, args=(stop_flag, proposal_number))
                        timer.start()

                    # Add the response to the list of responses for this proposal number
                    proposal_responses[proposal_number].append({
                        "node_id": node_id_received,
                        "action": action
                    })

                else:
                    print(f"Received unexpected message type: {message.get('type')}")

            except json.JSONDecodeError:
                print(f"Failed to decode broadcast message from {addr}. Ignoring.")
            except Exception as e:
                print(f"Error processing broadcast message from {addr}: {e}")

            finally:
                client_socket.close()

        except socket.timeout:
            # Timeout reached, check if any proposals have expired
            expired_proposals = []  # Store expired proposals to delete after iteration
            for proposal_number, stop_flag_value in stop_flag.items():
                if stop_flag_value == False:
                    continue
                else:
                    responses = proposal_responses[proposal_number]
                    if responses:
                        # Collect all actions for this proposal_number
                        actions = [response["action"] for response in responses]

                        # Check if all actions are the same
                        if all(action == actions[0] for action in actions):
                            action = actions[0]
                            perform_action(action, BankingService(db_name=f"banking_node_{node_id}.db"))
                            # Increase reputation for non-malicious nodes
                            for node in active_nodes:
                                if node not in malicious_nodes:
                                    print(f"Reputation increased for Node {node}.")
                                    increase_reputation(node)
                                else:
                                    print(f"Reputation decreased for Node {node}.")
                                    decrease_reputation(node)
                            print("Final list of active nodes: ", active_nodes)
                        else:
                            print(f"Inconsistent actions for proposal {proposal_number}: {actions}")

                    expired_proposals.append(proposal_number)

            # Remove expired proposals from stop_flag after the iteration
            for proposal_number in expired_proposals:
                del stop_flag[proposal_number]
        except Exception as e:
            print(f"Error accepting connection: {e}")
            continue



def listen_for_messages(node_id, db_name):
    """
    Function to listen for incoming connections from other nodes, handling actions and Paxos prepare messages.
    """

    global max_proposal
    host = "0.0.0.0"  # Listen on all interfaces
    port = 10000
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Node {node_id} listening on port {port}...")

    # Track the highest prepare proposal number seen

    while True:
        client_socket, addr = server_socket.accept()  # Accept incoming connection
        print(f"Connection from {addr} received.")

        # Receive the message from the client
        message_data = client_socket.recv(1024).decode()
        try:
            message = json.loads(message_data)
            print(f"Received message: {message}")

            response = ""

            if message.get("type") == "prepare":
                print(f"Received Prepare message from {addr}: {message}")
                # Handle Paxos Prepare messages
                proposal_number = message["proposal_number"]
                if proposal_number > max_proposal:
                    max_proposal = proposal_number
                    response = json.dumps({"status": "promise", "proposal_number": proposal_number})
                    print(f"Promised proposal {proposal_number}")
                else:
                    response = json.dumps({"status": "reject", "proposal_number": proposal_number})
                    print(f"Rejected proposal {proposal_number} (already promised {max_proposal})")
            elif message.get("type") == "propose":
                print(f"Received Propose message from {addr}: {message}")
                # Handle Paxos Propose messages
                proposal_number = message["proposal_number"]
                proposer_id = message["proposer_id"]
                if proposal_number == max_proposal:
                    # Perform the action
                    action = message["action"]
                    banking_service = BankingService(db_name=db_name)
                    is_possible = check_if_possible(action, banking_service)
                    if node_id == 4:
                        is_possible = "rejected"
                    if is_possible == "approved":
                        response = "approved"
                        print(f"Approved proposal {proposal_number}")

                        broadcast_verification_message(proposal_number, "approved", node_id,action, proposer_id)
                    else:
                        response = "rejected"
                        print(f"Rejected proposal {proposal_number} (not possible)")
                        broadcast_verification_message(proposal_number, "rejected", node_id,action, proposer_id)
                else:
                    response = "rejected"
                    print(f"Rejected proposal {proposal_number} (not the highest)")
                    broadcast_verification_message(proposal_number, "rejected", node_id,action, proposer_id)

            else:
                # Handle other messages, such as checking feasibility of actions
                banking_service = BankingService(db_name=db_name)
                response = check_if_possible(message, banking_service)

            # Send the response back to the sender
            client_socket.send(response.encode())

        except json.JSONDecodeError:
            print(f"Failed to decode message from {addr}. Ignoring.")
        except Exception as e:
            print(f"Error processing message from {addr}: {e}")

        finally:
            client_socket.close()

def increase_reputation(node_id):
    """Increase the reputation of a node."""
    global active_nodes
    active_nodes[str(node_id)]['reputation'] += 10
    print(f"Reputation increased for Node {node_id}. New reputation: {active_nodes[str(node_id)]['reputation']}")
    registry_url = f"http://{registry_ip}:5000/reputation/increase"
    try:
        response = requests.post(registry_url, json={"node_id": str(node_id)})
        if response.status_code == 200:
            print(f"Reputation increased for Node {node_id}.")
        else:
            print(f"Failed to increase reputation for Node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def decrease_reputation(node_id):
    """Decrease the reputation of a node."""
    global active_nodes
    active_nodes[str(node_id)]['reputation'] -= 20
    print(f"Reputation decreased for Node {node_id}. New reputation: {active_nodes[str(node_id)]['reputation']}")
    registry_url = f"http://{registry_ip}:5000/reputation/decrease"
    try:
        response = requests.post(registry_url, json={"node_id": node_id})
        if response.status_code == 200:
            print(f"Reputation decreased for Node {node_id}.")
        else:
            print(f"Failed to decrease reputation for Node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def perform_action(action, banking_service):
    """Perform the action on the local node using the shared banking service."""
    if 'action' not in action:
        print("Error: 'action' key missing in the received data.")
        return

    action_type = action['action']
    print(f"Performing action: {action}")

    try:
        if action_type == "deposit":
            if 'name' in action and 'amount' in action:
                banking_service.deposit(action["name"], action["amount"])
            else:
                print("Error: 'name' or 'amount' missing in deposit action.")
        
        elif action_type == "withdraw":
            if 'name' in action and 'amount' in action:
                banking_service.withdraw(action["name"], action["amount"])
            else:
                print("Error: 'name' or 'amount' missing in withdraw action.")
        
        elif action_type == "create_account":
            if 'name' in action and 'initial_balance' in action:
                banking_service.create_account(action["name"], action["initial_balance"])
            else:
                print("Error: 'name' or 'initial_balance' missing in create_account action.")
        
        else:
            print(f"Unknown action: {action_type}")
    
    except KeyError as e:
        print(f"Error: Missing required parameter {str(e)} for action '{action_type}'.")
    except Exception as e:
        print(f"An error occurred while performing the action: {str(e)}")

def check_if_possible(action, banking_service):
    """Check if the action is correct and possible to perform."""
    print("Sleeping for 10 seconds to simulate processing time...")
    print("USING BANKING NODE V2")
    time.sleep(10)
    if 'action' not in action:
        return "rejected"
    action_type = action['action']
    try:
        if action_type == "deposit":
            if 'name' in action and 'amount' in action:
                balance = banking_service.get_balance(action["name"])
                if balance is not None:
                    return "approved"
        elif action_type == "withdraw":
            if 'name' in action and 'amount' in action:
                balance = banking_service.get_balance(action["name"])
                if balance is not None and balance >= action["amount"]:
                    return "approved"
        elif action_type == "create_account":
            if 'name' in action and 'initial_balance' in action:
                return "approved"
        else:
            return "rejected"
    except KeyError:
        return "rejected"
    except Exception as e:
        print(f"An error occurred while checking the action: {str(e)}")
        return "rejected"

def register_with_registry(node_id):
    """
    Registers the node with the registry service.
    """
    global active_nodes

    registry_url = f"http://{registry_ip}:5000/register"  # Adjust URL as needed
    node_url = f"http://{node_ip}:{10000}"
    
    try:
        response = requests.post(registry_url, json={"node_id": node_id, "node_url": node_url})
        if response.status_code == 201:
            print(f"Node {node_id} registered successfully with the registry.")
            active_nodes = get_nodes()
            #send registration to active nodes
            if len(active_nodes) > 0:
                send_registration_to_active_nodes(active_nodes, node_id, node_url)
                active_nodes[str(node_id)] = {"url": node_url, "reputation": 100}
            else:
                active_nodes[str(node_id)] = {"url": node_url, "reputation": 100}
        elif response.status_code == 200:
            print(f"Node {node_id} already registered with the registry.")
            active_nodes = get_nodes()
            if len(active_nodes) > 0:
                send_registration_to_active_nodes(active_nodes, node_id, node_url)
                reputation = get_reputation_from_registry(node_id)
                active_nodes[str(node_id)] = {"url": node_url, "reputation": reputation}
            else:
                reputation = get_reputation_from_registry(node_id)
                active_nodes[str(node_id)] = {"url": node_url, "reputation": reputation}
        else:
            print(f"Failed to register node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def send_registration_to_active_nodes(active_nodes, node_id, node_ip):
    """
    Sends the registration information to all active nodes via socket communication.
    """
    for node in active_nodes:
        if str(node) != str(node_id):
            print(f"Adding Node {node} to the registry...")
            node_url = active_nodes[node]['url']
            if node_url:
                try:
                    # Extract IP and port from the node URL
                    node_ip_send= node_url.replace("http://", "").split(":")[0]
                    node_port_send = 5001

                    # Create a socket connection to the target node
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                        client_socket.connect((node_ip_send, node_port_send))
                        registration_data = {
                            node_id: {
                                "url": f"{node_ip}",
                                "reputation": 100
                            }
                        }
                        client_socket.send(json.dumps(registration_data).encode())
                        print(f"Sent registration data to Node {node}.")
                except Exception as e:
                    print(f"Error sending registration to node {node}: {e}")

def listen_for_node_registrations():
    """Function to listen for new node registration requests."""
    host = "0.0.0.0"
    port = 5001  # Listener port for incoming node registrations
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Registry listener started on port {port}...")

    while True:
        client_socket, addr = server_socket.accept()  # Accept incoming connection
        print(f"Registration request received from {addr}.")

        try:
            # Receive registration request data
            registration_data = client_socket.recv(1024).decode()
            registration_info = json.loads(registration_data)
            print(f"Received registration data: {registration_info}")

            # Process the registration
            for node_id, node_details in registration_info.items():
                if "url" in node_details and "reputation" in node_details:
                    active_nodes[node_id] = {
                        "url": node_details["url"],
                        "reputation": node_details["reputation"]
                    }
                    print(f"Node {node_id} registered with URL {node_details['url']} and reputation {node_details['reputation']}.")
                else:
                    print(f"Invalid registration data received: {node_details}")

            # Send acknowledgment to the client (node)
            response = {"status": "success", "message": "Node registration processed successfully."}
            client_socket.send(json.dumps(response).encode())
        except json.JSONDecodeError as e:
            print(f"Error decoding registration data: {e}")
            response = {"status": "error", "message": "Invalid registration data format."}
            client_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Unexpected error processing registration: {e}")
            response = {"status": "error", "message": "An error occurred during registration."}
            client_socket.send(json.dumps(response).encode())
        finally:
            print("Active nodes: ", active_nodes)
            client_socket.close()

def get_nodes():
    """
    Get the list of all nodes registered with the registry.
    """
    registry_url = f"http://{registry_ip}:5000/nodes"
    try:
        response = requests.get(registry_url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get nodes. Error: {response.text}")
            return {}
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")
        return {}
    
def get_reputation_from_registry(node_id):
    """
    Get the reputation of the current node from the registry.
    """
    registry_url = f"http://{registry_ip}:5000/reputation/{node_id}"  # Use path for node_id
    try:
        response = requests.get(registry_url)  # No need for params, as node_id is part of the URL
        if response.status_code == 200:
            return response.json().get("reputation", 0)
        else:
            print(f"Failed to get reputation for Node {node_id}. Error: {response.text}")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")
        return 0

    
 
def unregister_node(node_id):
    registry_url = f"http://{registry_ip}:5000/deregister"
    try:
        response = requests.post(registry_url, json={"node_id": node_id})
        if response.status_code == 200:
            print(f"Node {node_id} unregistered successfully.")
        else:
            print(f"Failed to unregister Node {node_id}: {response.status_code}")
    except Exception as e:
        print(f"Error during unregistration: {str(e)}")
    
def graceful_shutdown(node_id):
    print(f"Node {node_id} shutting down.")
    unregister_node(node_id)

def start_banking_service(node_id):
    db_name = f"banking_node_{node_id}.db"

    # Register the node with the registry
    register_with_registry(node_id)

    atexit.register(graceful_shutdown, node_id)

    #Get total nodes

    # Start the listener thread for this node
    listener_thread = threading.Thread(target=listen_for_messages, args=(node_id, db_name))
    listener_thread.daemon = True  # Ensure the thread exits when the main program exits
    listener_thread.start()

    # Start the registry listener thread
    registry_thread = threading.Thread(target=listen_for_node_registrations)
    registry_thread.daemon = True
    registry_thread.start()

    # Start the broadcast listener thread
    broadcast_thread = threading.Thread(target=listen_for_broadcasts, args=(node_id,))
    broadcast_thread.daemon = True
    broadcast_thread.start()

    # Start the learn listener thread
    learn_thread = threading.Thread(target=listen_for_learn_messages, args=(node_id,))
    learn_thread.daemon = True
    learn_thread.start()

    # Proceed with the menu and banking operations
    menu(node_id)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        node_id = int(input("Enter the node ID: "))
    else:
        node_id = int(sys.argv[1])  # Get node ID from the command-line argument

    start_banking_service(node_id)