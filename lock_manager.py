"""Strict-2PL lock table (S/X) with wait-for graph deadlock detection."""


class LockManager:
    def __init__(self):
        self.table = {}   # res -> {"holders": {tid: mode}, "queue": [(tid, mode)]}

    def _entry(self, res):
        return self.table.setdefault(res, {"holders": {}, "queue": []})

    @staticmethod
    def _compatible(holders, tid, mode):
        others = [m for t, m in holders.items() if t != tid]
        return not others or (mode == "S" and all(m == "S" for m in others))

    def acquire(self, tid, res, mode):
        e = self._entry(res)
        h = e["holders"]
        if tid in h and (h[tid] == "X" or mode == "S"):
            return True
        if self._compatible(h, tid, mode):
            h[tid] = mode
            return True
        e["queue"].append((tid, mode))
        return False

    def release_all(self, tid):
        """Drop every lock/wait of tid; return newly granted (tid, res, mode)."""
        granted = []
        for e in self.table.values():
            e["holders"].pop(tid, None)
            e["queue"] = [q for q in e["queue"] if q[0] != tid]
        for res, e in self.table.items():
            while e["queue"]:
                t, m = e["queue"][0]
                if not self._compatible(e["holders"], t, m):
                    break
                e["queue"].pop(0)
                e["holders"][t] = m
                granted.append((t, res, m))
        return granted

    def held_by(self, tid):
        return [(r, e["holders"][tid]) for r, e in self.table.items() if tid in e["holders"]]

    def x_holders(self, res):
        e = self.table.get(res)
        return [t for t, m in e["holders"].items() if m == "X"] if e else []

    def wait_for(self):
        edges = []
        for e in self.table.values():
            for w, m in e["queue"]:
                for h, hm in e["holders"].items():
                    if h != w and (m == "X" or hm == "X"):
                        edges.append((w, h))
        return edges

    def find_deadlock(self):
        g = {}
        for a, b in self.wait_for():
            g.setdefault(a, set()).add(b)
        state, stack = {}, []

        def dfs(n):
            state[n] = 1
            stack.append(n)
            for m in g.get(n, ()):
                if state.get(m) == 1:
                    return stack[stack.index(m):]
                if m not in state:
                    c = dfs(m)
                    if c:
                        return c
            stack.pop()
            state[n] = 2
            return None

        for n in list(g):
            if n not in state:
                c = dfs(n)
                if c:
                    return c
        return None
