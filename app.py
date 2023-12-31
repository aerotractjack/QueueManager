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

@app.route("/send_waiting_to_front", methods=["POST"])
def send_waiting_to_front():
    data = request.get_json()
    resp = qm.send_waiting_to_front(**data)
    output = {"success": resp}
    return jsonify(output)

@app.route("/send_waiting_to_back", methods=["POST"])
def send_waiting_to_back():
    data = request.get_json()
    resp = qm.send_waiting_to_back(**data)
    output = {"success": resp}
    return jsonify(output)

@app.route("/list_waiting_queues", methods=["GET"])
def list_waiting_queues():
    output = {"waiting_queues": qm.list_waiting_queues()}
    return jsonify(output)

@app.route("/read_waiting_queue_items", methods=["GET"])
def read_waiting_queue_items():
    args = request.args.to_dict()
    jsonList, itemNames = qm.read_waiting_queue_items(**args)
    output = {"waiting_queue_items": jsonList, "waiting_queue_item_names": itemNames}
    return jsonify(output)

@app.route("/waiting_queue_items/<queue_name>/<item_id>", methods=["DELETE"])
def delete_waiting_queue_item(queue_name, item_id):
    return jsonify({
        "success": qm.delete_waiting_queue_item(queue_name, item_id)
    })

@app.route("/read_inprocess_queue_items", methods=["GET"])
def read_inprocess_queue_items():
    jsonList, devices = qm.read_inprocess_queue_items()
    output = {"inprocess_queue_items": jsonList, "inprocess_queue_devices": devices}
    return jsonify(output)

@app.route("/inprocess_queue_items/<device_name>", methods=["DELETE"])
def delete_inprocess_queue_item(device_name):
    return jsonify({
        "success": qm.delete_inprocess_queue_item(device_name)
    })

@app.route("/read_tmpdir", methods=["GET"])
def read_tmpdir():
    args = request.args.to_dict()
    outputDict = qm.read_tmpdir(**args)
    return jsonify(outputDict)

@app.route("/read_failed_queue_items", methods=["GET"])
def read_failed_queue_items():
    args = request.args.to_dict()
    jsonList, itemNames = qm.read_failed_queue_items(**args)
    output = {"failed_queue_items": jsonList, "failed_queue_item_names": itemNames}
    return jsonify(output)

@app.route("/failed_queue_items/<item_id>", methods=["DELETE", "OPTIONS"])
def delete_failed_queue_item(item_id):
    return jsonify({
        "success": qm.delete_failed_queue_item(item_id)
    })

@app.route("/completed_queue_items", methods=["GET"])
def read_completed_queue_items():
    args = request.args.to_dict()
    jsonList, itemNames = qm.read_completed_queue_items(**args)
    output = {"completed_queue_items": jsonList, "completed_queue_item_names": itemNames}
    return jsonify(output)

@app.route("/completed_queue_items/<item_id>", methods=["DELETE", "OPTIONS"])
def delete_completed_queue_item(item_id):
    return jsonify({
        "success": qm.delete_completed_queue_item(item_id)
    })

@app.route("/colors", methods=["GET"])
def read_colors():
    return jsonify(qm.read_colors())

@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

if __name__ == "__main__":
    app.run(debug=True, port=7088)