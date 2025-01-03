from flask import Flask, request, jsonify

app = Flask(__name__)

# In-memory registry for storing node information and reputation
node_registry = {}

DEFAULT_REPUTATION = 100  # Default reputation for newly registered nodes

@app.route("/register", methods=["POST"])
def register_node():
    """
    Endpoint to register a node with its ID and URL.
    """
    data = request.json
    node_id = data.get("node_id")
    node_url = data.get("node_url")
    node_id = str(node_id)

    if not node_id or not node_url:
        return jsonify({"error": "Node ID and URL are required"}), 400

    if node_id in node_registry:
        existing_node = node_registry[node_id]
        if existing_node["url"] == node_url:
            return jsonify({"message": f"Node {node_id} already registered with URL {node_url}"}), 200
        else:
            return jsonify({"error": f"Node ID {node_id} already registered with a different URL"}), 409

    node_registry[node_id] = {
        "url": node_url,
        "reputation": DEFAULT_REPUTATION
    }
    return jsonify({"message": f"Node {node_id} registered successfully with URL {node_url}"}), 201

@app.route("/nodes", methods=["GET"])
def list_nodes():
    """
    Endpoint to list all registered nodes with their reputation.
    """
    return jsonify(node_registry), 200

@app.route("/total_nodes", methods=["GET"])
def total_nodes():
    """
    Endpoint to get the total number of registered nodes.
    """
    return jsonify({"total_nodes": len(node_registry)}), 200

@app.route("/deregister", methods=["POST"])
def deregister_node():
    """
    Endpoint to deregister a node.
    Expects JSON payload: { "node_id": <int> }
    """
    data = request.json
    if not data or "node_id" not in data:
        return jsonify({"error": "Invalid request. 'node_id' is required."}), 400

    node_id = data["node_id"]
    node_id = str(node_id)
    if node_id not in node_registry:
        return jsonify({"error": f"Node {node_id} is not registered."}), 404

    del node_registry[node_id]
    return jsonify({"message": f"Node {node_id} deregistered successfully."}), 200

# Functions to manage reputation
@app.route("/reputation/increase", methods=["POST"])
def increase_reputation():
    """
    Endpoint to increase a node's reputation.
    Expects JSON payload: { "node_id": <str>, "amount": <int> }
    """
    global node_registry
    data = request.json
    node_id = data.get("node_id")
    amount = data.get("amount", 10)  # Default increase amount is 10
    node_id = str(node_id)

    print(node_id)
    print(type(node_id))
    print(node_registry)

    if not node_id or node_id not in node_registry:
        return jsonify({"error": f"Node {node_id} is not registered."}), 404

    #Cant increase more than 100
    if node_registry[node_id]["reputation"] + amount > 100:
        node_registry[node_id]["reputation"] = 100
        return jsonify({"message": f"Reputation for Node {node_id} increased by {amount}.", 
                    "reputation": node_registry[node_id]["reputation"]}), 200
    
    node_registry[node_id]["reputation"] += amount
    return jsonify({"message": f"Reputation for Node {node_id} increased by {amount}.", 
                    "reputation": node_registry[node_id]["reputation"]}), 200

@app.route("/reputation/decrease", methods=["POST"])
def decrease_reputation():
    """
    Endpoint to decrease a node's reputation.
    Expects JSON payload: { "node_id": <str>, "amount": <int> }
    """
    global node_registry
    data = request.json
    node_id = data.get("node_id")
    amount = data.get("amount", 20)  # Default decrease amount is 10
    node_id = str(node_id)

    if not node_id or node_id not in node_registry:
        return jsonify({"error": f"Node {node_id} is not registered."}), 404

    node_registry[node_id]["reputation"] -= amount
    return jsonify({"message": f"Reputation for Node {node_id} decreased by {amount}.", 
                    "reputation": node_registry[node_id]["reputation"]}), 200

@app.route("/reputation/<node_id>", methods=["GET"])
def get_reputation(node_id):
    """
    Endpoint to get the reputation of a node.
    """
    node_id = str(node_id)
    if node_id not in node_registry:
        return jsonify({"error": f"Node {node_id} is not registered."}), 404

    reputation = node_registry[node_id]["reputation"]
    return jsonify({"node_id": node_id, "reputation": reputation}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)