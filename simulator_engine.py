"""Coordinates transactions, WAL, buffer, locks, crash injection and recovery."""
import functools, os, threading, time
from models import Transaction
from wal_manager import WALManager
from buffer_manager import BufferManager
from lock_manager import LockManager
from recovery_manager import RecoveryManager
from monitor import Monitor

CRASH_POINTS = {
    "BEFORE_COMMIT_FLUSH": "COMMIT record logged but not yet on disk (txn must be UNDONE)",
    "AFTER_COMMIT_FLUSH": "COMMIT durable, data pages still only in RAM (needs REDO)",
    "DURING_PAGE_FLUSH": "After WAL flush, before the page is written to disk",
    "DURING_CHECKPOINT": "Checkpoint record written but never made durable",
    "DURING_UNDO": "Crash in the middle of recovery's UNDO phase (CLRs make it safe)",
}


class CrashException(Exception):
    def __init__(self, point):
        super().__init__(point)
        self.point = point


def guarded(fn):
    """Thread-safe op; refuses work while crashed; turns CrashException into a crash."""
    @functools.wraps(fn)
    def wrapper(self, *a, **k):
        with self.lock:
            if self.status != "RUNNING":
                return {"ok": False, "error": f"System is {self.status}. Run recovery first."}
            try:
                res = fn(self, *a, **k)
                if self.auto_prevention and self.status == "RUNNING":
                    self.monitor.tick()
                return res
            except CrashException as c:
                self._crash(c.point)
                return {"ok": False, "crashed": True, "point": c.point,
                        "error": f"CRASH at {c.point}"}
    return wrapper


class SimulatorEngine:
    def __init__(self, data_dir="data", capacity=4, n_pages=6):
        self.data_dir, self.capacity, self.n_pages = data_dir, capacity, n_pages
        self.lock = threading.RLock()
        self.auto_prevention = True
        self._init(fresh=False)

    # ---------- setup ----------
    def _init(self, fresh):
        os.makedirs(self.data_dir, exist_ok=True)
        if fresh:
            for f in ("wal.log", "master.json", "disk.json"):
                p = os.path.join(self.data_dir, f)
                if os.path.exists(p):
                    os.remove(p)
        j = lambda f: os.path.join(self.data_dir, f)
        self.wal = WALManager(j("wal.log"), j("master.json"))
        self.buffer = BufferManager(j("disk.json"), self.wal, self.capacity, self.n_pages, hook=self._hit)
        self.locks = LockManager()
        self.txns, self.events = {}, []
        self.next_txn = max([r.txn_id for r in self.wal.records if r.txn_id] + [0]) + 1
        self.status, self.armed, self.throttled = "RUNNING", None, False
        self.last_recovery, self.last_checkpoint_time = None, time.time()
        self.monitor = Monitor(self)

    def reset(self):
        with self.lock:
            self._init(fresh=True)
            self.event("info", "System reset: empty log, all pages = 0")
            return {"ok": True}

    def event(self, level, msg):
        self.events.append({"t": time.strftime("%H:%M:%S"), "level": level, "msg": msg})
        del self.events[:-100]

    def _hit(self, point):
        if self.armed == point:
            self.armed = None
            raise CrashException(point)

    # ---------- failure injection ----------
    def arm(self, point):
        with self.lock:
            if point not in CRASH_POINTS:
                return {"ok": False, "error": "unknown crash point"}
            self.armed = point
            self.event("warn", f"Crash armed at {point}")
            return {"ok": True}

    def crash_now(self):
        with self.lock:
            if self.status == "CRASHED":
                return {"ok": False, "error": "already crashed"}
            self._crash("MANUAL")
            return {"ok": True}

    def _crash(self, point):
        self.wal.crash()            # unflushed log tail lost
        self.buffer.crash()         # RAM lost
        self.locks = LockManager()  # lock table lost
        for t in self.txns.values():
            if t.state in ("ACTIVE", "WAITING"):
                t.state, t.pending = "CRASHED", None
        self.status, self.armed = "CRASHED", None
        self.event("crit", f"CRASH ({point}): RAM, lock table and unflushed log tail lost")

    # ---------- transactions ----------
    @guarded
    def begin(self):
        if self.throttled:
            return {"ok": False, "error": "Throttled by prevention manager (too many active txns)"}
        tid, self.next_txn = self.next_txn, self.next_txn + 1
        t = self.txns[tid] = Transaction(tid)
        t.last_lsn = self.wal.append("BEGIN", txn_id=tid).lsn
        self.event("info", f"T{tid} began")
        return {"ok": True, "txn_id": tid}

    def _txn(self, tid):
        t = self.txns.get(tid)
        if not t:
            return None, {"ok": False, "error": f"no such txn T{tid}"}
        if t.state != "ACTIVE":
            return None, {"ok": False, "error": f"T{tid} is {t.state}"}
        return t, None

    def _apply_write(self, t, page, value):
        before = self.buffer.get(page).value
        rec = self.wal.append("UPDATE", txn_id=t.txn_id, page_id=page, prev_lsn=t.last_lsn,
                              before=before, after=value)
        self.buffer.update(page, value, rec.lsn)
        t.last_lsn = rec.lsn

    @guarded
    def write(self, tid, page, value):
        t, err = self._txn(tid)
        if err:
            return err
        if page not in self.buffer.disk:
            return {"ok": False, "error": f"unknown page {page}"}
        value = int(value)
        if self.locks.acquire(tid, page, "X"):
            self._apply_write(t, page, value)
            return {"ok": True, "status": "DONE"}
        t.state, t.pending, t.wait_since = "WAITING", ("write", page, value), time.time()
        self.event("warn", f"T{tid} waits for X lock on {page}")
        self._resolve_deadlocks()
        return {"ok": True, "status": t.state}

    @guarded
    def read(self, tid, page):
        t, err = self._txn(tid)
        if err:
            return err
        if page not in self.buffer.disk:
            return {"ok": False, "error": f"unknown page {page}"}
        if self.locks.acquire(tid, page, "S"):
            return {"ok": True, "status": "DONE", "value": self.buffer.get(page).value}
        t.state, t.pending, t.wait_since = "WAITING", ("read", page, None), time.time()
        self.event("warn", f"T{tid} waits for S lock on {page}")
        self._resolve_deadlocks()
        return {"ok": True, "status": t.state}

    @guarded
    def commit(self, tid):
        t, err = self._txn(tid)
        if err:
            return err
        t.last_lsn = self.wal.append("COMMIT", txn_id=tid, prev_lsn=t.last_lsn).lsn
        self._hit("BEFORE_COMMIT_FLUSH")
        self.wal.flush()                      # durable commit
        self._hit("AFTER_COMMIT_FLUSH")
        t.state = "COMMITTED"
        self.event("ok", f"T{tid} COMMITTED (commit record on disk)")
        self._release(tid)
        return {"ok": True}

    @guarded
    def abort(self, tid):
        t = self.txns.get(tid)
        if not t or t.state not in ("ACTIVE", "WAITING"):
            return {"ok": False, "error": f"T{tid} cannot be aborted"}
        self._abort(tid, "user request")
        return {"ok": True}

    def _abort(self, tid, reason):
        t = self.txns[tid]
        lsn = t.last_lsn
        while lsn:                            # walk the txn's log chain backwards
            rec = self.wal.get(lsn)
            if rec.type == "UPDATE":
                clr = self.wal.append("CLR", txn_id=tid, page_id=rec.page_id, prev_lsn=t.last_lsn,
                                      before=rec.after, after=rec.before, undo_next=rec.prev_lsn)
                self.buffer.update(rec.page_id, rec.before, clr.lsn)
                t.last_lsn, lsn = clr.lsn, rec.prev_lsn
            elif rec.type == "CLR":
                lsn = rec.undo_next
            else:
                lsn = rec.prev_lsn
        t.last_lsn = self.wal.append("ABORT", txn_id=tid, prev_lsn=t.last_lsn).lsn
        self.wal.flush()
        t.state, t.pending, t.note = "ABORTED", None, reason
        self.event("warn", f"T{tid} ROLLED BACK ({reason})")
        self._release(tid)

    def _release(self, tid):
        for t2, res, mode in self.locks.release_all(tid):
            t = self.txns[t2]
            if t.pending:
                kind, page, value = t.pending
                t.pending, t.state, t.wait_since = None, "ACTIVE", None
                if kind == "write":
                    self._apply_write(t, page, value)
                self.event("info", f"T{t2} got {mode} lock on {res} and resumed")

    def _resolve_deadlocks(self):
        while True:
            cycle = self.locks.find_deadlock()
            if not cycle:
                return
            victim = max(cycle)               # youngest txn loses
            self.event("crit", "DEADLOCK: " + " -> ".join(f"T{t}" for t in cycle + cycle[:1])
                       + f"; victim T{victim}")
            self._abort(victim, "deadlock victim")

    # ---------- checkpoint / flush ----------
    def _checkpoint(self):
        table = {t.txn_id: {"status": t.state, "last_lsn": t.last_lsn}
                 for t in self.txns.values() if t.state in ("ACTIVE", "WAITING")}
        self.wal.checkpoint(table, dict(self.buffer.dirty), hook=self._hit)
        self.last_checkpoint_time = time.time()

    @guarded
    def checkpoint(self):
        self._checkpoint()
        self.event("ok", f"Checkpoint at LSN {self.wal.checkpoint_lsn}")
        return {"ok": True, "lsn": self.wal.checkpoint_lsn}

    @guarded
    def flush(self, page=None):
        pages = [page] if page else list(self.buffer.dirty)
        n = sum(self.buffer.flush_page(p) for p in pages)
        self.event("info", f"Flushed {n} page(s) to disk (WAL flushed first)")
        return {"ok": True, "flushed": n}

    # ---------- recovery ----------
    def recover(self):
        with self.lock:
            if self.status != "CRASHED":
                return {"ok": False, "error": "nothing to recover: system is running"}
            self.status = "RECOVERING"
            self.event("info", "Recovery started: ANALYSIS -> REDO -> UNDO")
            try:
                rep = RecoveryManager(self.wal, self.buffer, self._hit).recover()
            except CrashException as c:
                self._crash(c.point)
                self.event("crit", "Crash during recovery. Run recovery again (CLRs keep it safe).")
                return {"ok": False, "crashed": True, "point": c.point}
            winners = set(rep["analysis"]["winners"])
            for t in self.txns.values():
                if t.state == "CRASHED":
                    t.state = "COMMITTED" if t.txn_id in winners else "ROLLED_BACK"
            self.next_txn = max([r.txn_id for r in self.wal.records if r.txn_id] + [0]) + 1
            self.last_checkpoint_time = time.time()
            self.status, self.last_recovery = "RUNNING", rep
            self.event("ok", f"Recovery done: {len(rep['redo'])} REDO, {len(rep['undo'])} UNDO, "
                             f"{len(rep['analysis']['winners'])} winner(s), {len(rep['analysis']['losers'])} loser(s)")
            return rep

    # ---------- view ----------
    def state(self):
        with self.lock:
            if self.status == "RUNNING" and self.auto_prevention:
                try:
                    self.monitor.tick()       # lets timeouts fire even when idle
                except CrashException as c:
                    self._crash(c.point)
            waits = {}
            for a, b in self.locks.wait_for():
                waits.setdefault(a, []).append(b)
            counts = {}
            for r in self.wal.records:
                if r.txn_id:
                    counts[r.txn_id] = counts.get(r.txn_id, 0) + 1
            total = sum(counts.values()) or 1
            m = self.monitor.metrics()
            return {
                "status": self.status, "armed": self.armed, "throttled": self.throttled,
                "auto_prevention": self.auto_prevention, "crash_points": CRASH_POINTS,
                "txns": [{"id": t.txn_id, "state": t.state, "note": t.note,
                          "locks": [f"{r}:{md}" for r, md in self.locks.held_by(t.txn_id)],
                          "waiting_for": waits.get(t.txn_id, []),
                          "log_records": counts.get(t.txn_id, 0),
                          "log_share": round(100 * counts.get(t.txn_id, 0) / total, 1)}
                         for t in self.txns.values()],
                "pages": [{"id": p, "disk": d["value"], "disk_lsn": d["page_lsn"],
                           "ram": self.buffer.frames[p].value if p in self.buffer.frames else None,
                           "ram_lsn": self.buffer.frames[p].page_lsn if p in self.buffer.frames else None,
                           "dirty": p in self.buffer.dirty}
                          for p, d in self.buffer.disk.items()],
                "buffer": {"capacity": self.buffer.capacity, "frames": len(self.buffer.frames),
                           "dirty": len(self.buffer.dirty), "disk_writes": self.buffer.disk_writes},
                "wal": [{"lsn": r.lsn, "type": r.type, "txn": r.txn_id, "page": r.page_id,
                         "before": r.before, "after": r.after, "undo_next": r.undo_next,
                         "durable": r.lsn <= self.wal.flushed_lsn} for r in self.wal.records[-60:]],
                "flushed_lsn": self.wal.flushed_lsn, "checkpoint_lsn": self.wal.checkpoint_lsn,
                "metrics": m, "risks": self.monitor.risks(m),
                "events": self.events[-40:], "last_recovery": self.last_recovery,
            }
