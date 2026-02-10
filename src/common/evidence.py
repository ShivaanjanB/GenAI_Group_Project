from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

@dataclass
class Evidence:
    source: str
    url: str
    retrieved_at: str
    as_of: str
    note: Optional[str] = None

@dataclass
class Field:
    value: Any = None
    evidence: Optional[Evidence] = None
    flagged: bool = False
    flag_reason: Optional[str] = None

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def flag(reason: str) -> Field:
    return Field(flagged=True, flag_reason=reason)

def ok(value: Any, evidence: Evidence) -> Field:
    return Field(value=value, evidence=evidence)
