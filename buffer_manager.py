"""Buffer pool: RAM frames over persistent disk pages (steal / no-force)."""
import json, os
from collections import OrderedDict
from models import Page


class BufferManager:
    def __init__(self, path, wal, capacity=4, n_pages=6, hook=None):
        self.path, self.wal, self.capacity, self.hook = path, wal, capacity, hook
        self.frames = OrderedDict()   # pid -> Page (RAM, LRU order)
        self.dirty = {}               # pid -> rec_lsn (first LSN that dirtied it)
        self.disk_writes = 0
        if os.path.exists(path):
            with open(path) as f:
                self.disk = json.load(f)
        else:
            self.disk = {f"P{i}": {"value": 0, "page_lsn": 0} for i in range(1, n_pages + 1)}
            self._save()

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.disk, f)
        os.replace(tmp, self.path)

    def get(self, pid):
        if pid not in self.disk:
            raise KeyError(f"unknown page {pid}")
        if pid in self.frames:
            self.frames.move_to_end(pid)
            return self.frames[pid]
        if len(self.frames) >= self.capacity:
            self._evict()
        d = self.disk[pid]
        self.frames[pid] = Page(pid, d["value"], d["page_lsn"])
        return self.frames[pid]

    def _evict(self):
        victim = next((p for p in self.frames if p not in self.dirty), next(iter(self.frames)))
        if victim in self.dirty:
            self.flush_page(victim)   # STEAL: may write uncommitted data
        del self.frames[victim]

    def update(self, pid, value, lsn):
        page = self.get(pid)
        page.value, page.page_lsn = value, lsn
        self.dirty.setdefault(pid, lsn)

    def flush_page(self, pid):
        page = self.frames.get(pid)
        if page is None or pid not in self.dirty:
            return False
        self.wal.flush(page.page_lsn)           # WAL rule: log first, page second
        if self.hook:
            self.hook("DURING_PAGE_FLUSH")
        self.disk[pid] = {"value": page.value, "page_lsn": page.page_lsn}
        self._save()
        self.dirty.pop(pid)
        self.disk_writes += 1
        return True

    def flush_all(self):
        for pid in list(self.dirty):
            self.flush_page(pid)

    def crash(self):
        """Power loss: RAM frames vanish, disk stays."""
        self.frames.clear()
        self.dirty.clear()
