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

@app.route("/list_waiting_queues", methods=["GET"])
def list_waiting_queues():
    output = {"waiting_queues": qm.list_waiting_queues()}
    return jsonify(output)

@app.route("/read_waiting_queue_items", methods=["GET"])
def read_waiting_queue_items():
    args = request.args.to_dict()
    output = {"waiting_queue_items": qm.read_waiting_queue_items(**args)}
    return jsonify(output)

@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

if __name__ == "__main__":
    app.run(debug=True, port=7088)