import socket
import json
import threading

def send_to_node(host, port, message):
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))
        client_socket.send(json.dumps(message).encode())
        client_socket.close()
    except ConnectionRefusedError:
        print(f"Node at {host}:{port} is not reachable.")

def handle_connection(client_socket, db_name, handle_request):
    data = client_socket.recv(1024).decode()
    message = json.loads(data)
    handle_request(message, db_name)
    client_socket.close()

def start_listener(node_id, db_name, handle_request):
    host = "127.0.0.1"
    port = 5000 + node_id

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Node {node_id} listening on port {port}...")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr}")
        # Start a new thread to handle the request
        threading.Thread(target=handle_connection, args=(client_socket, db_name, handle_request)).start()
