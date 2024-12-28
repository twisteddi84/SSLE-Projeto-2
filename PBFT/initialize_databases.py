from shared.banking_service import BankingService

def initialize_database(node_id):
    db_name = f"databases/banking_node_{node_id}.db"
    service = BankingService(db_name)
    print(f"Database for Node {node_id} initialized at {db_name}.")

if __name__ == "__main__":
    total_nodes = 3  # Adjust if you have more nodes
    for node_id in range(1, total_nodes + 1):
        initialize_database(node_id)