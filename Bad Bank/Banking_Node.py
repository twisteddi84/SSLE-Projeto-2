import sqlite3
import json
import socket
import threading
import time

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


def menu(node_id, total_nodes):
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
            send_action_to_all_nodes(node_id, total_nodes, action)

        elif choice == "2":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to deposit: "))
            action = {"action": "deposit", "name": name, "amount": amount}
            send_action_to_all_nodes(node_id, total_nodes, action)

        elif choice == "3":
            name = input("Enter account holder's name: ")
            amount = float(input("Enter amount to withdraw: "))
            action = {"action": "withdraw", "name": name, "amount": amount}
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
    for acceptor_id in range(1, total_nodes + 1):
        if acceptor_id != node_id:
            send_to_node(acceptor_id, action)
        else:
            perform_action(action, BankingService(f"banking_node_{acceptor_id}.db"))

def send_to_node(acceptor_id, action):
    """Send an action to a specific node."""
    host = "127.0.0.1"
    port = 5000 + acceptor_id

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        client_socket.send(json.dumps(action).encode())
        client_socket.close()
    except ConnectionRefusedError:
        print(f"Node {acceptor_id} is not reachable.")

def listen_for_actions(node_id, db_name):
    """Function to listen for incoming connections from other nodes."""
    host = "127.0.0.1"
    port = 5000 + node_id
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

        perform_action(action, banking_service)  # Apply the action

        client_socket.close()

def start_banking_service(node_id, total_nodes):
    db_name = f"banking_node_{node_id}.db"

    # Start the listener thread for this node
    listener_thread = threading.Thread(target=listen_for_actions, args=(node_id, db_name))
    listener_thread.daemon = True  # Ensure the thread exits when the main program exits
    listener_thread.start()

    # Proceed with the menu and banking operations
    menu(node_id, total_nodes)


if __name__ == "__main__":
    node_id = int(input("Enter your node ID: "))  # Node ID
    total_nodes = int(input("Enter total number of nodes: "))  # Total nodes

    start_banking_service(node_id, total_nodes)