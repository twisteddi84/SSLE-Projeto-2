import multiprocessing
from banking_node import BankingNode

def start_node(node_id, total_nodes, db_name, stop_event):
    node = BankingNode(node_id, total_nodes, db_name)
    while not stop_event.is_set():
        node.run()

def main():
    total_nodes = 3
    db_name = "bank_db"
    
    # Event to signal all nodes to stop
    stop_event = multiprocessing.Event()

    processes = []
    for node_id in range(1, total_nodes + 1):
        p = multiprocessing.Process(target=start_node, args=(node_id, total_nodes, db_name, stop_event))
        processes.append(p)
        p.start()

    try:
        # Let the processes run for a while (or until some condition is met)
        # In a real scenario, you would have some condition to stop the nodes
        # Here we simulate a stop after some time (e.g., 10 seconds)
        import time
        time.sleep(10)
    finally:
        # Signal all processes to stop
        stop_event.set()

        # Join all processes to ensure they finish before main exits
        for p in processes:
            p.join()

if __name__ == "__main__":
    main()