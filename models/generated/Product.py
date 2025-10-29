#Product.py
from __future__ import annotations
from models.generated.ProductId import ProductId
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class Product:
    symbol: Optional[str] = None
    securityType: Optional[str] = None
    callPut: Optional[str] = None
    expiryYear: Optional[int] = None
    expiryMonth: Optional[int] = None
    expiryDay: Optional[int] = None
    strikePrice: Optional[int] = None
    productId: Optional[ProductId] = None
