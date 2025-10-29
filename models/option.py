from dataclasses import dataclass
from typing import Optional, Dict
from datetime import datetime

@dataclass
class OptionGreeks:
    rho: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    iv: Optional[float] = None
    currentValue: Optional[bool] = None

@dataclass
class ProductId:
    symbol: str
    typeCode: str

@dataclass
class Product:
    symbol: str
    securityType: str
    callPut: Optional[str] = None
    expiryYear: Optional[int] = None
    expiryMonth: Optional[int] = None
    expiryDay: Optional[int] = None
    strikePrice: Optional[float] = None
    productId: Optional[ProductId] = None

@dataclass
class Quick:
    lastTrade: Optional[float] = None
    lastTradeTime: Optional[int] = None
    change: Optional[float] = None
    changePct: Optional[float] = None
    volume: Optional[int] = None
    quoteStatus: Optional[str] = None

@dataclass
class OptionContract:
    symbol: str
    optionType: str
    strikePrice: float
    displaySymbol: str
    osiKey: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    bidSize: Optional[int] = None
    askSize: Optional[int] = None
    inTheMoney: Optional[str] = None
    volume: Optional[int] = None
    openInterest: Optional[int] = None
    netChange: Optional[float] = None
    lastPrice: Optional[float] = None
    quoteDetail: Optional[str] = None
    optionCategory: Optional[str] = None
    timeStamp: Optional[int] = None
    adjustedFlag: Optional[bool] = None
    OptionGreeks: Optional[OptionGreeks] = None
    quick: Optional[Quick] = None
    product: Optional[Product] = None
    expiryDate: Optional[datetime] = None
    nearPrice: Optional[float] = None