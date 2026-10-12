"""Core data entities (plain dataclasses)."""
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class LogRecord:
    lsn: int
    type: str                      # BEGIN UPDATE CLR COMMIT ABORT CHECKPOINT
    txn_id: Optional[int] = None
    page_id: Optional[str] = None
    prev_lsn: Optional[int] = None  # previous record of the same txn
    before: Optional[int] = None    # undo image
    after: Optional[int] = None     # redo image
    undo_next: Optional[int] = None  # CLR only: next record to undo
    data: Optional[dict] = None     # CHECKPOINT payload

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(d):
        return LogRecord(**d)


@dataclass
class Page:
    page_id: str
    value: int = 0
    page_lsn: int = 0               # LSN of the last update applied


@dataclass
class Transaction:
    txn_id: int
    state: str = "ACTIVE"           # ACTIVE WAITING COMMITTED ABORTED CRASHED ROLLED_BACK
    last_lsn: Optional[int] = None
    wait_since: Optional[float] = None
    pending: Optional[tuple] = None  # operation blocked on a lock
    note: str = ""
