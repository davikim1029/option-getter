from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class OptionFeatures:
    symbol: str
    optionType: int                     # 1 = CALL, 0 = PUT
    strikePrice: float
    lastPrice: float
    bid: Optional[float]
    ask: Optional[float]
    bidSize: Optional[int]
    askSize: Optional[int]
    openInterest: Optional[int]
    volume: Optional[int]
    inTheMoney: int                     # 1 = yes, 0 = no
    nearPrice: Optional[float]
    delta: Optional[float]
    gamma: Optional[float]
    theta: Optional[float]
    vega: Optional[float]
    rho: Optional[float]
    iv: Optional[float]
    daysToExpiration: Optional[int]
    spread: Optional[float]
    midPrice: Optional[float]
    moneyness: Optional[float]
    sentiment: float
    timestamp: datetime

    def to_dict(self) -> dict:
        """Return a clean dict representation (for ML model or JSON export)."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d
