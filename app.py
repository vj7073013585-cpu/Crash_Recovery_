"""Flask REST API + dashboard."""
from flask import Flask, jsonify, request, render_template
from simulator_engine import SimulatorEngine
from scenarios import SCENARIOS

app = Flask(__name__)
engine = SimulatorEngine()
engine.event("info", "Simulator ready")


def body():
    return request.get_json(silent=True) or {}


@app.get("/")
def index():
    return render_template("dashboard.html")


@app.get("/api/state")
def state():
    return jsonify(engine.state())


@app.post("/api/begin")
def begin():
    return jsonify(engine.begin())


@app.post("/api/write")
def write():
    b = body()
    return jsonify(engine.write(int(b["txn"]), b["page"], b["value"]))


@app.post("/api/read")
def read():
    b = body()
    return jsonify(engine.read(int(b["txn"]), b["page"]))


@app.post("/api/commit")
def commit():
    return jsonify(engine.commit(int(body()["txn"])))


@app.post("/api/abort")
def abort():
    return jsonify(engine.abort(int(body()["txn"])))


@app.post("/api/checkpoint")
def checkpoint():
    return jsonify(engine.checkpoint())


@app.post("/api/flush")
def flush():
    return jsonify(engine.flush(body().get("page")))


@app.post("/api/arm")
def arm():
    return jsonify(engine.arm(body().get("point")))


@app.post("/api/crash")
def crash():
    return jsonify(engine.crash_now())


@app.post("/api/recover")
def recover():
    return jsonify(engine.recover())


@app.post("/api/reset")
def reset():
    return jsonify(engine.reset())


@app.post("/api/auto")
def auto():
    engine.auto_prevention = bool(body().get("on", True))
    return jsonify({"ok": True, "auto_prevention": engine.auto_prevention})


@app.post("/api/scenario/<name>")
def scenario(name):
    if name not in SCENARIOS:
        return jsonify({"ok": False, "error": "unknown scenario"}), 404
    return jsonify({"ok": True, "message": SCENARIOS[name](engine)})


if __name__ == "__main__":
    app.run(debug=False, port=5000, threaded=True)
