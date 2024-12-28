from shared.banking_service import BankingService
from shared.pbft_utils import send_to_node
import json


class BankingNode:
    def __init__(self, node_id, total_nodes, db_name):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.state = "NONE"
        self.view = -1
        self.db_name = db_name
        self.commit_count = {}
        self.executed_transactions = set()
        self.banking_service = BankingService(db_name)

    def handle_request(self, message):
        """
        Handles incoming PBFT messages based on their action type and processes banking transactions.
        """
        global commit_count, executed_transactions
        action = message.get("action")
        transaction = message.get("transaction")

        if not action or not transaction:
            print("[ERROR] Invalid message. Ignoring.")
            return

        print(f"[DEBUG] Current state: {self.state}, Received action: {action}")

        if action == "pre-prepare" and self.state == "NONE":
            print(f"[DEBUG] Received pre-prepare, broadcasting to replicas...")
            self.state = "PRE_PREPARE_SENT"
            self.broadcast("prepare", transaction)

        elif action == "prepare" and self.state == "PRE_PREPARE_SENT":
            print(f"[DEBUG] Received prepare, broadcasting commit to replicas...")
            self.state = "PREPARE_SENT"
            self.broadcast("commit", transaction)

        elif action == "commit" and self.state == "PREPARE_SENT":
            print(f"[DEBUG] Received commit, processing transaction...")
            self.process_commit(transaction)

        else:
            print(f"[WARNING] Unknown or invalid action: {action}, Current state: {self.state}")

    def broadcast(self, action, transaction):
        """
        Broadcast a message to all nodes except the current one.
        """
        for replica_id in range(1, self.total_nodes + 1):
            if replica_id != self.node_id:
                send_to_node("127.0.0.1", 5000 + replica_id, {"action": action, "transaction": transaction})

    def process_commit(self, transaction):
        """
        Process the commit and execute the transaction if a quorum is reached.
        """
        transaction_id = hash(json.dumps(transaction, sort_keys=True))  # Unique transaction ID

        if transaction_id in self.executed_transactions:
            print(f"[DEBUG] Transaction {transaction_id} already executed. Skipping.")
            return

        # Initialize commit count for this transaction if not already done
        if transaction_id not in self.commit_count:
            self.commit_count[transaction_id] = set()

        # Add the node ID to the set of commits for this transaction
        self.commit_count[transaction_id].add(self.node_id)

        # Check if quorum is reached
        if len(self.commit_count[transaction_id]) >= 2 * (self.total_nodes // 3) + 1:  # Adjust based on configuration
            print(f"[DEBUG] Quorum reached for transaction: {transaction}")
            print("[DEBUG] Executing transaction...")

            # Execute the transaction
            transaction_type = transaction.get("type")
            if transaction_type == "create_account":
                self.banking_service.create_account(transaction.get("name"), transaction.get("balance"))
            elif transaction_type == "deposit":
                self.banking_service.deposit(transaction.get("name"), transaction.get("amount"))
            elif transaction_type == "withdraw":
                self.banking_service.withdraw(transaction.get("name"), transaction.get("amount"))
            else:
                print(f"[WARNING] Unknown transaction type: {transaction_type}")

            # Mark the transaction as executed
            self.executed_transactions.add(transaction_id)

        else:
            print(f"[DEBUG] Waiting for more commits for transaction {transaction_id}.")

    def run(self):
        """
        Start the node and handle incoming requests.
        """
        while True:
            message = self.consume_msg()
            self.handle_request(message)

    def consume_msg(self):
        """
        This method would simulate consuming a message from the network.
        You can replace this with your own messaging logic.
        """
        # For testing, return a dummy message here. You can replace this with real message consumption.
        return {
            "action": "pre-prepare",
            "transaction": {
                "type": "create_account",
                "name": "Alice",
                "balance": 100.0
            }
        }
