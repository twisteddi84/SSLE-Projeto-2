from shared.pbft_utils import start_listener
from shared.banking_service import BankingService
from shared.pbft_utils import send_to_node
import threading
import json

node_id = 2  # Change to 3 for node_3.py
db_name = f"databases/banking_node_{node_id}.db"
banking_service = BankingService(db_name)
total_nodes = 3

def handle_request(message, db_name):
    action = message.get("action")
    if not action:
        print("[ERROR] Received message without an 'action' field. Ignoring message.")
        return

    print(f"[DEBUG] Node {node_id} received action: {action}")
    print(f"[DEBUG] Full message: {json.dumps(message, indent=4)}")

    if action == "pre-prepare":
        print("[DEBUG] Node {node_id} processing 'pre-prepare' action. Broadcasting 'prepare' to other replicas...")
        transaction = message.get("transaction")
        if transaction:
            for replica_id in range(2, total_nodes + 1):
                if replica_id != node_id:  # Skip self
                    send_to_node("127.0.0.1", 5000 + replica_id, {"action": "prepare", "transaction": transaction, "node_id": node_id})
            print(f"[DEBUG] Node {node_id} broadcasted 'prepare' messages.")
        else:
            print("[WARNING] 'pre-prepare' message received without a transaction field. Ignoring.")

    elif action == "prepare":
        print(f"[DEBUG] Node {node_id} processing 'prepare' action with transaction: {message.get('transaction')}")
        transaction = message.get("transaction")
        if transaction:
            print(f"[DEBUG] Node {node_id} validating transaction: {transaction}")
            for replica_id in range(1, total_nodes + 1):  # Commit sent to all nodes, including self
                if replica_id != node_id:  # Skip self
                    send_to_node("127.0.0.1", 5000 + replica_id, {"action": "commit", "transaction": transaction, "node_id": node_id})
            print(f"[DEBUG] Node {node_id} sent 'commit' message to all replicas.")
        else:
            print("[WARNING] 'prepare' message received without a transaction field. Ignoring.")

    elif action == "commit":
        print(f"[DEBUG] Node {node_id} received 'commit'. Executing transaction...")
        transaction = message.get("transaction")
        if transaction:
            transaction_type = transaction.get("type")
            if transaction_type == "create_account":
                banking_service.create_account(transaction.get("name"), transaction.get("balance"))
            elif transaction_type == "deposit":
                banking_service.deposit(transaction.get("name"), transaction.get("amount"))
            elif transaction_type == "withdraw":
                banking_service.withdraw(transaction.get("name"), transaction.get("amount"))
            else:
                print(f"[WARNING] Node {node_id} received unknown transaction type: {transaction_type}")
        else:
            print("[WARNING] 'commit' message received without a transaction field. Ignoring.")

    else:
        print(f"[WARNING] Node {node_id} received unknown action: {action}")

if __name__ == "__main__":
    print(f"[INFO] Node {node_id} starting with database {db_name}")
    listener_thread = threading.Thread(target=start_listener, args=(node_id, db_name, handle_request))
    listener_thread.start()