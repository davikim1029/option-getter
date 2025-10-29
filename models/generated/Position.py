#Position.py
from __future__ import annotations
from models.generated.Product import Product
from models.generated.Quick import Quick
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Position:
    positionId: Optional[int] = None
    osiKey: Optional[str] = None
    symbolDescription: Optional[str] = None
    dateAcquired: Optional[int] = None
    pricePaid: Optional[float] = None
    commissions: Optional[float] = None
    otherFees: Optional[float] = None
    quantity: Optional[int] = None
    positionIndicator: Optional[str] = None
    positionType: Optional[str] = None
    daysGain: Optional[float] = None
    daysGainPct: Optional[float] = None
    marketValue: Optional[float] = None
    totalCost: Optional[float] = None
    totalGain: Optional[float] = None
    totalGainPct: Optional[float] = None
    pctOfPortfolio: Optional[float] = None
    costPerShare: Optional[float] = None
    todayCommissions: Optional[float] = None
    todayFees: Optional[float] = None
    todayPricePaid: Optional[float] = None
    todayQuantity: Optional[int] = None
    adjPrevClose: Optional[float] = None
    lotsDetails: Optional[str] = None
    quoteDetails: Optional[str] = None
    Product: Optional[Product] = None
    Quick: Optional[Quick] = None
