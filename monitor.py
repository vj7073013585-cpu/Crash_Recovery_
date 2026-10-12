"""Metrics -> risk classification -> automatic prevention."""
import time

LIMITS = {"wal_since_checkpoint": 12, "buffer_dirty_ratio": 0.75,
          "lock_wait_s": 10, "active_txns": 6}


class RiskDetector:
    def __init__(self, limits):
        self.limits = limits

    def classify(self, m):
        out = []
        for key, limit in self.limits.items():
            v = m.get(key, 0)
            level = "critical" if v >= limit else "warn" if v >= 0.7 * limit else "ok"
            out.append({"metric": key, "value": round(v, 2), "limit": limit, "level": level})
        return out


class PreventionManager:
    def __init__(self, engine):
        self.e = engine

    def respond(self, m, risks):
        e = self.e
        by = {r["metric"]: r for r in risks}
        if by["wal_since_checkpoint"]["level"] == "critical":
            e._checkpoint()
            e.event("ok", f"AUTO: WAL grew to {m['wal_since_checkpoint']} records -> checkpoint taken")
        if by["buffer_dirty_ratio"]["level"] == "critical":
            safe = [p for p in list(e.buffer.dirty) if not e.locks.x_holders(p)]
            n = sum(e.buffer.flush_page(p) for p in safe)
            if n:
                e.event("ok", f"AUTO: dirty buffer {m['buffer_dirty_ratio']:.0%} -> flushed {n} safe page(s)")
        if by["lock_wait_s"]["level"] == "critical":
            waiting = [t for t in e.txns.values() if t.state == "WAITING"]
            if waiting:
                victim = min(waiting, key=lambda t: t.wait_since)
                e.event("crit", f"AUTO: T{victim.txn_id} waited too long -> rolled back")
                e._abort(victim.txn_id, "lock wait timeout")
        throttle = by["active_txns"]["level"] == "critical"
        if throttle != e.throttled:
            e.throttled = throttle
            e.event("warn" if throttle else "ok",
                    "AUTO: throttling new transactions" if throttle else "AUTO: throttle lifted")


class Monitor:
    def __init__(self, engine):
        self.e = engine
        self.limits = dict(LIMITS)
        self.detector = RiskDetector(self.limits)
        self.prevention = PreventionManager(engine)

    def metrics(self):
        e, now = self.e, time.time()
        waiting = [t for t in e.txns.values() if t.state == "WAITING"]
        active = [t for t in e.txns.values() if t.state in ("ACTIVE", "WAITING")]
        return {
            "wal_records": len(e.wal.records),
            "wal_unflushed": len(e.wal.records) - e.wal.flushed_lsn,
            "wal_since_checkpoint": len(e.wal.records) - e.wal.checkpoint_lsn,
            "buffer_usage": len(e.buffer.frames) / e.buffer.capacity,
            "buffer_dirty_ratio": len(e.buffer.dirty) / e.buffer.capacity,
            "waiting_txns": len(waiting),
            "lock_wait_s": max([now - t.wait_since for t in waiting], default=0),
            "active_txns": len(active),
            "checkpoint_age_s": now - e.last_checkpoint_time,
        }

    def risks(self, m=None):
        return self.detector.classify(m or self.metrics())

    def tick(self):
        m = self.metrics()
        self.prevention.respond(m, self.detector.classify(m))
