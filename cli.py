"""Interactive terminal demo:  python cli.py"""
from simulator_engine import SimulatorEngine, CRASH_POINTS
from scenarios import SCENARIOS

HELP = """begin | write T PAGE VAL | read T PAGE | commit T | abort T | checkpoint | flush [PAGE]
arm POINT | crash | recover | state | scenario NAME | reset | help | quit
crash points: %s
scenarios: %s""" % (", ".join(CRASH_POINTS), ", ".join(SCENARIOS))


def show(e):
    s = e.state()
    print(f"\n== {s['status']}  flushed_lsn={s['flushed_lsn']} checkpoint_lsn={s['checkpoint_lsn']} armed={s['armed']}")
    print("PAGE  RAM    DISK   DIRTY")
    for p in s["pages"]:
        print(f"{p['id']:<5} {str(p['ram']):<6} {p['disk']:<6} {'*' if p['dirty'] else ''}")
    print("TXN   STATE        LOCKS")
    for t in s["txns"]:
        print(f"T{t['id']:<4} {t['state']:<12} {' '.join(t['locks'])}")
    print("LSN TYPE       TXN PAGE  BEFORE->AFTER  DURABLE")
    for r in s["wal"][-15:]:
        print(f"{r['lsn']:<3} {r['type']:<10} {str(r['txn'] or ''):<3} {str(r['page'] or ''):<5} "
              f"{str(r['before']) + '->' + str(r['after']) if r['page'] else '':<14} {'yes' if r['durable'] else 'RAM only'}")
    for ev in s["events"][-6:]:
        print(f"[{ev['t']}] {ev['level'].upper():5} {ev['msg']}")


def main():
    e = SimulatorEngine()
    print(HELP)
    while True:
        try:
            parts = input("\ncrash-sim> ").split()
        except (EOFError, KeyboardInterrupt):
            break
        if not parts:
            continue
        c, a = parts[0].lower(), parts[1:]
        try:
            if c in ("quit", "exit"): break
            elif c == "help": print(HELP)
            elif c == "begin": print(e.begin())
            elif c == "write": print(e.write(int(a[0].lstrip("Tt")), a[1], a[2]))
            elif c == "read": print(e.read(int(a[0].lstrip("Tt")), a[1]))
            elif c == "commit": print(e.commit(int(a[0].lstrip("Tt"))))
            elif c == "abort": print(e.abort(int(a[0].lstrip("Tt"))))
            elif c == "checkpoint": print(e.checkpoint())
            elif c == "flush": print(e.flush(a[0] if a else None))
            elif c == "arm": print(e.arm(a[0].upper()))
            elif c == "crash": print(e.crash_now())
            elif c == "recover":
                r = e.recover()
                print({k: v for k, v in r.items() if k not in ("redo", "undo")}); print("REDO:", r.get("redo")); print("UNDO:", r.get("undo"))
            elif c == "scenario": print(SCENARIOS[a[0]](e))
            elif c == "reset": print(e.reset())
            elif c == "state": pass
            else: print("unknown command"); continue
        except (IndexError, KeyError, ValueError) as ex:
            print("usage error:", ex); continue
        show(e)


if __name__ == "__main__":
    main()
