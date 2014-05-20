from gevent import monkey
monkey.patch_all()

import time, random, uuid, json
from flask import Flask, request, jsonify
from flask_sockets import Sockets


app = Flask(__name__)
app.debug = False
ws = Sockets(app)

idle_workers = set()
active_workers = set()

results = {}


@ws.route('/tasks')
def tasks(ws):
    idle_workers.add(ws)
    while True:
        msg = ws.receive()
        if msg:
             data = json.loads(msg)
             if 'task' not in data:
                 continue  # consider all invalid messages as heartbeats
             task = data['task']
             results[task] = data
             active_workers.discard(ws)
             idle_workers.add(ws)
        else:
             break 
    # at this point worker has disconnected
    idle_workers.discard(ws)
    active_workers.discard(ws)

@app.route('/schedule/', methods=['POST'])
def schedule():
    code = request.json['code']
    inputs = request.json['inputs']
    worker, task = get_worker(idle_workers, active_workers)
    if worker:
        worker.send(json.dumps({
            "code": code,
            "inputs": inputs,
            "task": task
        }))
        return jsonify({"task": task}), 200
    else:
        return "", 503

@app.route('/results/<task>', methods=['GET'])
def result(task):
    if task not in results:
        return "", 404
    else:
        result = results[task]
        if result:
            results[task] = None
            return jsonify(result)
        else:
            return "", 410

def get_worker(idle, active):
    l_idle = list(idle)
    task_uuid = uuid.uuid4()
    if len(l_idle) > 0:
        worker = random.choice(l_idle)
        idle.discard(worker)
        active.add(worker)
    else:
        worker = None
    return worker, str(task_uuid)

