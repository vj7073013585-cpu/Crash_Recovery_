"""ARIES recovery: Analysis -> REDO (repeat history) -> UNDO (with CLRs)."""
import time


class RecoveryManager:
    def __init__(self, wal, buffer, hook=None):
        self.wal, self.buf, self.hook = wal, buffer, hook

    def recover(self):
        t0 = time.time()
        wal, buf = self.wal, self.buf

        # ---- 1. ANALYSIS: rebuild txn table + dirty page table from last checkpoint
        txns, dpt, start = {}, {}, 0
        if wal.checkpoint_lsn:
            cp = wal.get(wal.checkpoint_lsn)
            start = cp.lsn
            for k, v in cp.data["txns"].items():
                txns[int(k)] = {"status": "ACTIVE", "last_lsn": v["last_lsn"]}
            dpt = dict(cp.data["dpt"])
        for rec in wal.records[start:]:
            if rec.type in ("BEGIN", "UPDATE", "CLR"):
                t = txns.setdefault(rec.txn_id, {"status": "ACTIVE", "last_lsn": None})
                t["last_lsn"] = rec.lsn
                if rec.type != "BEGIN":
                    dpt.setdefault(rec.page_id, rec.lsn)
            elif rec.type in ("COMMIT", "ABORT"):
                t = txns.setdefault(rec.txn_id, {"status": "ACTIVE", "last_lsn": rec.lsn})
                t["status"] = "COMMITTED" if rec.type == "COMMIT" else "ABORTED"
        winners = sorted(t for t, v in txns.items() if v["status"] == "COMMITTED")
        losers = {t: v["last_lsn"] for t, v in txns.items() if v["status"] == "ACTIVE"}
        redo_start = min(dpt.values()) if dpt else None
        analysis = {"checkpoint_lsn": wal.checkpoint_lsn, "winners": winners,
                    "losers": sorted(losers), "dirty_pages": dpt, "redo_start": redo_start}

        # ---- 2. REDO: repeat history for pages that may be stale on disk
        redone, skipped = [], 0
        if redo_start:
            for rec in wal.records[redo_start - 1:]:
                if rec.type in ("UPDATE", "CLR") and rec.page_id in dpt and rec.lsn >= dpt[rec.page_id]:
                    page = buf.get(rec.page_id)
                    if page.page_lsn < rec.lsn:
                        page.value, page.page_lsn = rec.after, rec.lsn
                        buf.dirty.setdefault(rec.page_id, rec.lsn)
                        redone.append({"lsn": rec.lsn, "txn": rec.txn_id, "page": rec.page_id, "value": rec.after})
                    else:
                        skipped += 1   # disk already has this change

        # ---- 3. UNDO: roll back losers, newest record first, writing CLRs
        undone, to_undo, last = [], dict(losers), dict(losers)
        while to_undo:
            tid = max(to_undo, key=lambda t: to_undo[t])
            rec = wal.get(to_undo[tid])
            if rec.type == "UPDATE":
                clr = wal.append("CLR", txn_id=tid, page_id=rec.page_id, prev_lsn=last[tid],
                                 before=rec.after, after=rec.before, undo_next=rec.prev_lsn)
                last[tid] = clr.lsn
                page = buf.get(rec.page_id)
                page.value, page.page_lsn = rec.before, clr.lsn
                buf.dirty.setdefault(rec.page_id, clr.lsn)
                wal.flush()                      # CLR durable before we go on
                undone.append({"lsn": rec.lsn, "clr": clr.lsn, "txn": tid,
                               "page": rec.page_id, "restored": rec.before})
                nxt = rec.prev_lsn
                if self.hook:
                    self.hook("DURING_UNDO")
            elif rec.type == "CLR":
                nxt = rec.undo_next              # never undo an undo
            else:
                nxt = rec.prev_lsn
            if nxt:
                to_undo[tid] = nxt
            else:
                del to_undo[tid]
                wal.append("ABORT", txn_id=tid, prev_lsn=last[tid])
        wal.flush()

        # ---- finish: consistent state => flush + clean checkpoint
        buf.flush_all()
        wal.checkpoint({}, {})
        return {"ok": True, "analysis": analysis, "redo": redone, "redo_skipped": skipped,
                "undo": undone, "ms": round((time.time() - t0) * 1000, 2)}
