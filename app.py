from flask import Flask, request, jsonify
from QueueManager import QueueManager

app = Flask(__name__)
qm = QueueManager("./example_queue_structure")

@app.route("/swap_waiting_q_items", methods=["POST"])
def swap_waiting_q_items():
    data = request.get_json()
    resp = qm.swap_waiting_queue_items(**data)
    output = {"success": resp}
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True, port=7088)