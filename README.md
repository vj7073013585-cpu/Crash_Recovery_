# Crash Simulator — Real-Time DBMS Monitoring, Failure Prevention & Crash Recovery

Team Vertex · OSDBMS-T023

## Run
    pip install -r requirements.txt
    python app.py          # dashboard at http://127.0.0.1:5000
    python cli.py          # interactive terminal demo
    python test_recovery.py

## Module map (matches Phase-II report)
| File | Role |
|---|---|
| app.py | Flask REST API + dashboard |
| simulator_engine.py | Transaction lifecycle, crash injection, recovery orchestration |
| models.py | LogRecord, Page, Transaction dataclasses |
| buffer_manager.py | RAM frames over disk pages (steal / no-force, WAL rule on flush) |
| wal_manager.py | LSNs, durable flush, fuzzy checkpoints, master record |
| recovery_manager.py | ARIES Analysis → REDO → UNDO (CLRs) |
| lock_manager.py | S/X locks, wait-for graph, deadlock cycle detection |
| monitor.py | Metrics, RiskDetector, PreventionManager (auto checkpoint / flush / rollback / throttle) |
| scenarios.py | Repeatable demos: redo, undo, undo_crash, checkpoint, deadlock, prevention |

## Crash points
BEFORE_COMMIT_FLUSH, AFTER_COMMIT_FLUSH, DURING_PAGE_FLUSH, DURING_CHECKPOINT, DURING_UNDO, or "Crash now".

## Notes / limitations
* Pages hold a single integer; one write = one log record (simplified model).
* A crash discards RAM (frames, lock table, unflushed log tail) inside the engine; durable state lives in `data/` (wal.log, disk.json, master.json), so the dashboard stays up. Moving the engine into a separate worker process (ProcessManager) is the next step.
* Prevention runs on every operation and on each dashboard poll (no background thread yet).
