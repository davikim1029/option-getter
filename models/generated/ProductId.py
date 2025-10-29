#ProductId.py
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class ProductId:
    symbol: Optional[str] = None
    typeCode: Optional[str] = None
