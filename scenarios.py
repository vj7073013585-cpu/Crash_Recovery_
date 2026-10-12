"""Repeatable demo scenarios."""


def redo_demo(e):
    e.reset()
    t1 = e.begin()["txn_id"]
    e.write(t1, "P1", 100); e.write(t1, "P2", 200)
    e.arm("AFTER_COMMIT_FLUSH"); e.commit(t1)
    return "T1 committed durably but its pages never reached disk. Crashed -> press Recover to watch REDO."


def undo_demo(e):
    e.reset()
    t1 = e.begin()["txn_id"]
    e.write(t1, "P1", 111); e.write(t1, "P2", 222)
    e.flush("P1")                                  # STEAL: uncommitted data on disk
    t2 = e.begin()["txn_id"]
    e.write(t2, "P3", 333); e.commit(t2)           # committed, but only in RAM
    e.crash_now()
    return "T1 (uncommitted) leaked P1 to disk; T2 committed only in RAM. Recover -> REDO T2, UNDO T1."


def undo_crash_demo(e):
    undo_demo(e)
    e.arm("DURING_UNDO")
    return "Same as undo demo, but the first Recover will crash mid-UNDO. Run Recover twice."


def checkpoint_demo(e):
    e.reset()
    t1 = e.begin()["txn_id"]; e.write(t1, "P1", 10); e.commit(t1)
    e.checkpoint()
    t2 = e.begin()["txn_id"]; e.write(t2, "P2", 20); e.commit(t2)
    t3 = e.begin()["txn_id"]; e.write(t3, "P3", 30)
    e.crash_now()
    return "Checkpoint after T1. Analysis starts from it; T2 is a winner, T3 a loser."


def deadlock_demo(e):
    e.reset()
    t1 = e.begin()["txn_id"]; t2 = e.begin()["txn_id"]
    e.write(t1, "P1", 1); e.write(t2, "P2", 2)
    e.write(t1, "P2", 3); e.write(t2, "P1", 4)
    return "T1 and T2 each wanted the other's page. Deadlock detected, youngest txn rolled back."


def prevention_demo(e):
    e.reset()
    t1 = e.begin()["txn_id"]
    for i in range(1, 16):
        e.write(t1, "P1", i)
    return "WAL grew past its limit; the prevention manager took a checkpoint automatically."


SCENARIOS = {"redo": redo_demo, "undo": undo_demo, "undo_crash": undo_crash_demo,
             "checkpoint": checkpoint_demo, "deadlock": deadlock_demo, "prevention": prevention_demo}
