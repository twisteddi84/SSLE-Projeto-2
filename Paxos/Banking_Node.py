import atexit
import signal
import sqlite3
import json
import socket
import threading
import time

import requests

node_ip = "127.0.0.1"
registry_ip = "127.0.0.1"

active_nodes = {}

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

            total_nodes = len(active_nodes)

            majority_approval = wait_for_majority_approval(node_id, total_nodes, action)
            if majority_approval:
                banking_service.create_account(name, initial_balance)
                send_action_to_all_nodes(node_id, total_nodes, action)

        elif choice == "2":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to deposit: "))
            action = {"action": "deposit", "name": name, "amount": amount}

            total_nodes = len(active_nodes)

            majority_approval = wait_for_majority_approval(node_id, total_nodes, action)
            if majority_approval:
                banking_service.deposit(name, amount)
                send_action_to_all_nodes(node_id, total_nodes, action)

        elif choice == "3":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to withdraw: "))
            action = {"action": "withdraw", "name": name, "amount": amount}

            total_nodes = len(active_nodes)

            majority_approval = wait_for_majority_approval(node_id, total_nodes, action)
            if majority_approval:
                banking_service.withdraw(name, amount)
                send_action_to_all_nodes(node_id, total_nodes, action)

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

def wait_for_majority_approval(node_id, total_nodes, action):
    """Wait for a majority of acceptors to approve the action using threading."""
    approvals = 0
    required_approvals = total_nodes // 2 + 1
    all_nodes = get_nodes()
    responses = {}
    lock = threading.Lock()

    def request_approval(acceptor_id,acceptor_url):
        """Send request and process the response."""
        nonlocal approvals
        responses[acceptor_id] = send_and_wait_for_response(acceptor_id, action,acceptor_url)
        with lock:
            #Node id 4 is always rejected
            if responses[acceptor_id] == "approved":
                approvals += 1

            if acceptor_id == 4:
                responses[acceptor_id] = "rejected"

    # Thread to check the node's own action
    def check_own_action():
        responses[node_id] = check_if_possible(action, BankingService(db_name=f"banking_node_{node_id}.db"))
        nonlocal approvals
        if responses[node_id] == "approved":
            with lock:
                approvals += 1

    # Start a thread for checking the local node's action
    local_thread = threading.Thread(target=check_own_action)
    local_thread.start()

    threads = []

    for acceptor_id in all_nodes.keys():
        if int(acceptor_id) != node_id:
            acceptor_url = all_nodes[acceptor_id]['url']
            thread = threading.Thread(target=request_approval, args=(int(acceptor_id),acceptor_url))
            threads.append(thread)
            thread.start()

    # Wait for all threads to finish
    local_thread.join()  # Wait for the local thread to finish
    for thread in threads:
        thread.join()

    node_dict = get_nodes()

    #Just use the node_id each reputation is higher than 50
    for node in node_dict.keys():
        if int(node) != node_id:
            if node_dict[node]['reputation'] < 50:
                print(f"Node {node} reputation is lower than 50. Node {node} will be ignored.")
                del responses[int(node)]
                approvals -= 1

    # Check if majority is reached
    if approvals >= required_approvals:
        print(f"Majority approved the action: {action}")
        print(f"Responses: {responses}")
        # Once majority is reached, execute the action locally and send 'learn'
        send_learn_to_all_nodes(node_id, action)
        # Increase reputation for the aproved nodes
        for acceptor_id in responses.keys():
            if responses[acceptor_id] == "approved":
                increase_reputation(acceptor_id)
        #Decrease reputation for the rejected nodes
        for acceptor_id in responses.keys():
            if responses[acceptor_id] == "rejected":
                decrease_reputation(acceptor_id)
        return True
    
    #Increase reputation for the rejected nodes
    for acceptor_id in responses.keys():
        if responses[acceptor_id] == "rejected":
            increase_reputation(acceptor_id)
    
    #Decrease reputation for the approved nodes
    for acceptor_id in responses.keys():
        if responses[acceptor_id] == "approved":
            decrease_reputation(acceptor_id)
    

    print(f"Action not approved by the majority. Responses: {responses}")
    return False

def send_learn_to_all_nodes(node_id, action):
    """Send the 'learn' message to all nodes to finalize the action."""

    for node in active_nodes.keys():
        if int(node) != node_id:
            send_to_node(active_nodes[node]['url'], {"action": "learn", "data": action})

def increase_reputation(node_id):
    """Increase the reputation of a node."""
    registry_url = f"http://{registry_ip}:5000/reputation/increase"
    try:
        response = requests.post(registry_url, json={"node_id": node_id})
        if response.status_code == 200:
            print(f"Reputation increased for Node {node_id}.")
        else:
            print(f"Failed to increase reputation for Node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def decrease_reputation(node_id):
    """Decrease the reputation of a node."""
    registry_url = f"http://{registry_ip}:5000/reputation/decrease"
    try:
        response = requests.post(registry_url, json={"node_id": node_id})
        if response.status_code == 200:
            print(f"Reputation decreased for Node {node_id}.")
        else:
            print(f"Failed to decrease reputation for Node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def send_and_wait_for_response(node_id, action,node_url ,timeout=15):
    """
    Send an action to a node and wait for the response with a timeout.
    If the node doesn't respond within the timeout, assume rejection.
    """
    host = node_url.split(":")[1].replace("/", "")
    port = int(node_url.split(":")[2])

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(timeout)  # Set the timeout in seconds
        client_socket.connect((host, port))
        client_socket.send(json.dumps(action).encode())

        data = client_socket.recv(1024).decode()  # Wait for response
        print(f"Received response from Node {node_id}: {data}")
        client_socket.close()
        return data  # Return 'approved' or 'rejected' based on the response

    except (socket.timeout, ConnectionRefusedError):
        # Handle timeout or unreachable node
        print(f"Node {node_id} did not respond in time or is not reachable.")
        return "rejected"

    finally:
        client_socket.close()

def perform_action(action, banking_service):
    """Perform the action on the local node using the shared banking service."""
    if 'action' not in action:
        print("Error: 'action' key missing in the received data.")
        return

    action_type = action['action']

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

def send_action_to_all_nodes(node_id, total_nodes, action):
    """Send the action to all other nodes for consensus."""
    for node in active_nodes.keys():
        if int(node) != node_id:
            send_to_node(active_nodes[node]['url'], {"action": "learn", "data": action})

def send_to_node(acceptor_url, action):
    """Send an action to a specific node."""
    host = acceptor_url.split(":")[1].replace("/", "")
    port = int(acceptor_url.split(":")[2])
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        client_socket.send(json.dumps(action).encode())
        client_socket.close()
    except ConnectionRefusedError:
        print(f"Node {acceptor_url} is not reachable.")

def check_if_possible(action, banking_service):
    """Check if the action is correct and possible to perform."""
    print("Sleeping for 10 seconds to simulate processing time...")
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

def listen_for_actions(node_id, db_name):
    """Function to listen for incoming connections from other nodes."""
    host = "0.0.0.0" # Listen on all interfaces
    port = 5000
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Node {node_id} listening on port {port}...")

    while True:
        client_socket, addr = server_socket.accept()  # Accept incoming connection
        print(f"Connection from {addr} received.")

        # Receive action from the client
        action_data = client_socket.recv(1024).decode()
        action = json.loads(action_data)
        print(f"Received action: {action}")

        # Create a new BankingService instance for this thread
        banking_service = BankingService(db_name=db_name)

        # Process the action based on its type
        if action["action"] == "learn":
            perform_action(action["data"], banking_service)  # Apply the action
            response = "learned"  # Acknowledge that the node has applied the action
            #Print menu again
            print(f"\n--- Banking Service Menu for Node {node_id} ---")
            print("1. Create Account")
            print("2. Deposit Money")
            print("3. Withdraw Money")
            print("4. Check Balance")
            print("5. Exit")
        else:
            response = check_if_possible(action, banking_service)


        client_socket.send(response.encode())
        client_socket.close()

def register_with_registry(node_id):
    """
    Registers the node with the registry service.
    """
    registry_url = f"http://{registry_ip}:5000/register"  # Adjust URL as needed
    node_url = f"http://{node_ip}:{5000}"
    try:
        response = requests.post(registry_url, json={"node_id": node_id, "node_url": node_url})
        if response.status_code == 201:
            print(f"Node {node_id} registered successfully with the registry.")
            active_nodes = get_nodes()
            #send registration to active nodes
            if len(active_nodes) > 1:
                send_registration_to_active_nodes(active_nodes, node_id, node_url)
        elif response.status_code == 200:
            print(f"Node {node_id} already registered with the registry.")
        else:
            print(f"Failed to register node {node_id}. Error: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to the registry: {e}")

def send_registration_to_active_nodes(active_nodes, node_id, node_url):
    """
    Sends the registration information to all active nodes.
    """
    for node in active_nodes:
        node_url = node.get("node_url")  # Assuming the node URL is in the "node_url" field
        if node_url:
            try:
                # Send the registration information to each active node
                registration_data = {"node_id": node_id, "node_url": f"http://{node_ip}:{5000}"}
                response = requests.post(f"{node_url}/register", json=registration_data)
                if response.status_code == 200:
                    print(f"Registration of Node {node_id} sent to active node {node['node_id']}.")
                else:
                    print(f"Failed to send registration to Node {node['node_id']}.")
            except requests.exceptions.RequestException as e:
                print(f"Error sending registration to node {node['node_id']}: {e}")

def listen_for_node_registrations():
    """Function to listen for new node registration requests."""
    host = "0.0.0.0"
    port = 5001  # You can choose a specific port for the registry listener
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind to the port and start listening
    server_socket.bind((host, port))
    server_socket.listen(5)
    print("Registry listener started on port 5000...")

    while True:
        client_socket, addr = server_socket.accept()  # Accept incoming connection
        print(f"Registration request received from {addr}.")

        # Receive registration request data
        registration_data = client_socket.recv(1024).decode()
        registration_info = json.loads(registration_data)
        print(f"Received registration data: {registration_info}")

        node_id = registration_info.get("node_id")
        node_url = registration_info.get("node_url")

        # Process the registration
        if node_id and node_url:
            # Store node_id and node_url in the registry (implement registry logic)
            print(f"Node {node_id} registered with URL {node_url}.")
            # You could update a list of registered nodes here
            active_nodes[node_id] = {"url": node_url, "reputation": 100}

            # Send acknowledgment to the client (node)
            response = {"status": "success", "message": f"Node {node_id} registered successfully."}
            client_socket.send(json.dumps(response).encode())
        else:
            # Invalid registration data
            response = {"status": "error", "message": "Invalid registration data."}
            client_socket.send(json.dumps(response).encode())

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
    listener_thread = threading.Thread(target=listen_for_actions, args=(node_id, db_name))
    listener_thread.daemon = True  # Ensure the thread exits when the main program exits
    listener_thread.start()

    # Start the registry listener thread
    registry_thread = threading.Thread(target=listen_for_node_registrations)
    registry_thread.daemon = True
    registry_thread.start()

    # Proceed with the menu and banking operations
    menu(node_id)


if __name__ == "__main__":
    node_id = int(input("Enter your node ID: "))  # Node ID

    start_banking_service(node_id)