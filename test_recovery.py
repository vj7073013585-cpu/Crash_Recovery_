"""Run:  python test_recovery.py   (uses a temp data dir)"""
import tempfile
from simulator_engine import SimulatorEngine
import scenarios as sc


def fresh():
    return SimulatorEngine(tempfile.mkdtemp())


def vals(e):
    return {p["id"]: p["disk"] for p in e.state()["pages"]}


def test_redo():
    e = fresh(); sc.redo_demo(e)
    assert vals(e)["P1"] == 0                       # not on disk yet
    r = e.recover()
    assert vals(e)["P1"] == 100 and vals(e)["P2"] == 200 and len(r["redo"]) == 2


def test_undo_and_redo():
    e = fresh(); sc.undo_demo(e)
    assert vals(e)["P1"] == 111                     # uncommitted leaked to disk
    r = e.recover()
    v = vals(e)
    assert v["P1"] == 0 and v["P2"] == 0 and v["P3"] == 333
    assert r["analysis"]["losers"] == [1] and r["analysis"]["winners"] == [2]


def test_crash_during_recovery():
    e = fresh(); sc.undo_crash_demo(e)
    assert e.recover().get("crashed")
    assert e.status == "CRASHED"
    e.recover()
    v = vals(e)
    assert v["P1"] == 0 and v["P2"] == 0 and v["P3"] == 333


def test_before_commit_flush_loses_txn():
    e = fresh(); t = e.begin()["txn_id"]; e.write(t, "P1", 5)
    e.arm("BEFORE_COMMIT_FLUSH"); e.commit(t)
    e.recover()
    assert vals(e)["P1"] == 0


def test_checkpoint():
    e = fresh(); sc.checkpoint_demo(e)
    r = e.recover()
    assert r["analysis"]["checkpoint_lsn"] > 0
    v = vals(e)
    assert v["P1"] == 10 and v["P2"] == 20 and v["P3"] == 0


def test_deadlock():
    e = fresh(); sc.deadlock_demo(e)
    s = {t["id"]: t["state"] for t in e.state()["txns"]}
    assert s == {1: "ACTIVE", 2: "ABORTED"}


def test_prevention_checkpoint():
    e = fresh(); sc.prevention_demo(e)
    assert e.wal.checkpoint_lsn > 0


def test_restart_from_disk():
    d = tempfile.mkdtemp(); e = SimulatorEngine(d)
    t = e.begin()["txn_id"]; e.write(t, "P4", 9); e.commit(t); e.crash_now()
    e2 = SimulatorEngine(d); e2.crash_now(); e2.recover()   # new process, same files
    assert vals(e2)["P4"] == 9


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("PASS", n)
