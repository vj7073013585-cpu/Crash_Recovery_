"""Write-ahead log: LSNs, durable flush, checkpoints."""
import json, os
from models import LogRecord


class WALManager:
    def __init__(self, path, master_path):
        self.path, self.master_path = path, master_path
        self.records = []           # durable records + volatile tail
        self.flushed_lsn = 0
        self.checkpoint_lsn = 0
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.records = [LogRecord.from_dict(json.loads(l)) for l in f if l.strip()]
        self.flushed_lsn = len(self.records)
        if os.path.exists(self.master_path):
            with open(self.master_path) as f:
                self.checkpoint_lsn = json.load(f)["checkpoint_lsn"]

    @property
    def next_lsn(self):
        return len(self.records) + 1

    def append(self, type, **kw):
        rec = LogRecord(lsn=self.next_lsn, type=type, **kw)
        self.records.append(rec)    # sits in the volatile tail until flushed
        return rec

    def get(self, lsn):
        return self.records[lsn - 1]

    def flush(self, up_to=None):
        up_to = len(self.records) if up_to is None else min(up_to, len(self.records))
        if up_to <= self.flushed_lsn:
            return
        with open(self.path, "a") as f:
            for r in self.records[self.flushed_lsn:up_to]:
                f.write(json.dumps(r.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.flushed_lsn = up_to

    def crash(self):
        """Power loss: the unflushed tail disappears."""
        del self.records[self.flushed_lsn:]

    def checkpoint(self, txn_table, dpt, hook=None):
        """Fuzzy checkpoint: snapshot txn table + dirty page table."""
        rec = self.append("CHECKPOINT", data={"txns": txn_table, "dpt": dpt})
        if hook:
            hook("DURING_CHECKPOINT")   # crash here => checkpoint never became durable
        self.flush()
        tmp = self.master_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"checkpoint_lsn": rec.lsn}, f)
        os.replace(tmp, self.master_path)
        self.checkpoint_lsn = rec.lsn
        return rec
