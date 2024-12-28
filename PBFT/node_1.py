import json
from shared.pbft_utils import send_to_node, start_listener
from shared.banking_service import BankingService
import threading

node_id = 1
total_nodes = 3
db_name = f"databases/banking_node_{node_id}.db"
banking_service = BankingService(db_name)
commit_count = {}
executed_transactions = set()
f = (total_nodes - 1) // 3

def handle_request(message, db_name):
    """
    Handles incoming PBFT messages based on their action type.
    """
    global commit_count, executed_transactions, f
    action = message.get("action")
    if not action:
        print("[ERROR] Received message without an 'action' field. Ignoring message.")
        return

    print(f"[DEBUG] Received action: {action}")
    print(f"[DEBUG] Full message: {json.dumps(message, indent=4)}")

    if action == "pre-prepare":
        print("Pre-Prepare received, broadcasting to replicas...")
        transaction = message.get("transaction")
        if transaction:
            for replica_id in range(2, total_nodes + 1):
                send_to_node("127.0.0.1", 5000 + replica_id, {"action": "prepare", "transaction": transaction})
        else:
            print("[WARNING] 'pre-prepare' message received without a transaction field. Ignoring.")

    elif action == "prepare":
        print("Prepare received, broadcasting commit to replicas...")
        transaction = message.get("transaction")
        if transaction:
            for replica_id in range(2, total_nodes + 1):
                send_to_node("127.0.0.1", 5000 + replica_id, {"action": "commit", "transaction": transaction})
        else:
            print("[WARNING] 'prepare' message received without a transaction field. Ignoring.")

    elif action == "commit":
        print("Commit received, processing transaction...")
        transaction = message.get("transaction")
        if transaction:
            transaction_id = hash(json.dumps(transaction, sort_keys=True))  # Unique ID for the transaction

            # Ensure the transaction is not already executed
            if transaction_id in executed_transactions:
                print(f"[DEBUG] Transaction {transaction_id} already executed. Skipping.")
                return

            # Initialize commit count for this transaction if not already done
            if transaction_id not in commit_count:
                commit_count[transaction_id] = set()

            # Add the node ID to the set of commits for this transaction
            node_id = message.get("node_id")
            if node_id:
                commit_count[transaction_id].add(node_id)
                print(f"[DEBUG] Commit count for transaction {transaction_id}: {len(commit_count[transaction_id])}")

            # Check if quorum is reached
            if len(commit_count[transaction_id]) >= 2 * f + 1:  # Check if quorum is reached based on total nodes
                print(f"[DEBUG] Quorum reached for transaction: {transaction}")
                print("[DEBUG] Executing transaction...")

                # Execute the transaction (Create Account, Deposit, Withdraw)
                transaction_type = transaction.get("type")
                if transaction_type == "create_account":
                    banking_service.create_account(transaction.get("name"), transaction.get("balance"))
                elif transaction_type == "deposit":
                    banking_service.deposit(transaction.get("name"), transaction.get("amount"))
                elif transaction_type == "withdraw":
                    banking_service.withdraw(transaction.get("name"), transaction.get("amount"))
                else:
                    print(f"[WARNING] Unknown transaction type: {transaction_type}")

                # Mark the transaction as executed
                executed_transactions.add(transaction_id)

            # Clean up commit count for the transaction
            if transaction_id in commit_count:
                del commit_count[transaction_id]
        else:
            print("[WARNING] 'commit' message received without a transaction field. Ignoring.")
    else:
        print(f"[WARNING] Unknown action: {action}")

def broadcast_to_all_replicas(transaction):
    """
    Broadcast a pre-prepare message to all replica nodes.
    The transaction includes the necessary details for execution.
    """
    print("Broadcasting pre-prepare to replicas...")
    for replica_id in range(2, total_nodes + 1):
        message = {
            "action": "pre-prepare",
            "transaction": transaction,
            "node_id": node_id  # Include sender node ID for tracking
        }
        send_to_node("127.0.0.1", 5000 + replica_id, message)
    print("Pre-prepare broadcast completed.")

if __name__ == "__main__":
    listener_thread = threading.Thread(target=start_listener, args=(node_id, db_name, handle_request))
    listener_thread.daemon = True
    listener_thread.start()

    while True:
        print("1. Create Account")
        print("2. Deposit")
        print("3. Withdraw")
        choice = input("Enter your choice: ")

        if choice == "1":
            name = input("Name: ")
            balance = float(input("Initial Balance: "))
            transaction = {"type": "create_account", "name": name, "balance": balance}
            broadcast_to_all_replicas(transaction)
            #banking_service.create_account(name, balance)

        elif choice == "2":
            name = input("Name: ")
            amount = float(input("Deposit Amount: "))
            transaction = {"type": "deposit", "name": name, "amount": amount}
            broadcast_to_all_replicas(transaction)
            #banking_service.deposit(name, amount)

        elif choice == "3":
            name = input("Name: ")
            amount = float(input("Withdraw Amount: "))
            transaction = {"type": "withdraw", "name": name, "amount": amount}
            broadcast_to_all_replicas(transaction)
            #banking_service.withdraw(name, amount)
