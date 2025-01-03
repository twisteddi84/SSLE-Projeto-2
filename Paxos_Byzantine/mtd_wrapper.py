import os
import random
import subprocess
import time
import psutil
from threading import Timer

# List of available binaries (make sure the paths are correct)
binary_versions = [
    'dist/Banking_Node_v1',
    'dist/Banking_Node_v2'
]

# List to keep track of active processes
running_processes = []

# Function to terminate any running processes of Banking_Node
def terminate_running_processes():
    global running_processes
    for proc in running_processes:
        try:
            print(f"Terminating {proc.pid}...")
            proc.terminate()  # Send termination signal
            proc.wait()  # Ensure the process has been terminated before proceeding
        except psutil.NoSuchProcess:
            print(f"Process {proc.pid} already terminated.")
        except psutil.AccessDenied:
            print(f"Access denied when trying to terminate process {proc.pid}.")
    # Clear the list after termination
    running_processes = []

# Function to stop the process after a given timeout (60 seconds)
def stop_process_after_timeout(process, timeout=60):
    timer = Timer(timeout, terminate_process, [process])  # Create a Timer that calls terminate_process after timeout
    timer.start()

# Function to terminate the process
def terminate_process(process):
    print(f"Terminating process {process.pid} due to timeout.")
    process.terminate()
    process.wait()  # Wait for the process to terminate gracefully

# Function to randomly select and execute a binary with node_id argument
def execute_random_binary():
    # Randomly select one binary from the list
    binary_to_execute = random.choice(binary_versions)

    # Define the node_id (You can generate or pass it dynamically)
    node_id = 1

    print(f"Executing {binary_to_execute} with node_id={node_id}...")
    process = subprocess.Popen([binary_to_execute, str(node_id)])

    # Add the process to the list of running processes
    running_processes.append(process)

    # Start a timer to terminate the process after 60 seconds
    stop_process_after_timeout(process, timeout=60)

    # Wait for the process to complete (this will block, but we want to manage the timeout separately)
    process.wait()
    print(f"Process {binary_to_execute} completed.")

# Function to start MTD execution
def start_mtd_execution():
    while True:
        # Stop any currently running processes
        terminate_running_processes()

        # Execute a random binary
        execute_random_binary()

        # Sleep for a random time between executions (optional)
        print("Waiting for the next execution...")
        time.sleep(10)  # Wait for 10 seconds before re-executing

if __name__ == "__main__":
    start_mtd_execution()