#Quick.py
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Quick:
    lastTrade: Optional[float] = None
    lastTradeTime: Optional[int] = None
    change: Optional[float] = None
    changePct: Optional[float] = None
    volume: Optional[int] = None
    quoteStatus: Optional[str] = None
