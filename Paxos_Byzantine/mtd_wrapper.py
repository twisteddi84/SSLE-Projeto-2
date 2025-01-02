import os
import random
import subprocess
import time

# List of available binaries (make sure the paths are correct)
binary_versions = [
    '/dist/Banking_Node_v1',
    '/dist/Banking_Node_v2'
]

# Function to randomly select and execute a binary with node_id argument
def execute_random_binary():
    # Randomly select one binary from the list
    binary_to_execute = random.choice(binary_versions)

    # Define the node_id (You can generate or pass it dynamically)
    node_id = 1

    print(f"Executing {binary_to_execute} with node_id={node_id}...")
    subprocess.run([binary_to_execute, str(node_id)])

# Function to start MTD execution
def start_mtd_execution():
    while True:
        execute_random_binary()
        # Sleep for a random time between executions (optional)
        time.sleep(random.randint(5, 30))  # Randomly sleep between 5 and 30 seconds

if __name__ == "__main__":
    start_mtd_execution()