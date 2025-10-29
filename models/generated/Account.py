#Account.py
from __future__ import annotations
from models.generated.Position import Position
from dataclasses import dataclass
from typing import Optional, List

@dataclass
class PortfolioAccount:
    accountId: Optional[str] = None
    Position: Optional[List[Position]] = None
    totalPages: Optional[int] = None
    
    @staticmethod
    def from_dict(data: dict) -> PortfolioAccount:
        # Convert raw dict into AccountPortfolio, including Position objects
        positions = [Position(**p) for p in data.get("Position", [])]
        return PortfolioAccount(
            accountId=data.get("accountId"),
            Position=positions,
            totalPages=data.get("totalPages")
        )

@dataclass 
class Account:
    accountId: str
    accountIdKey: str
    accountMode: str
    accountDesc: str
    accountName: str
    accountType: str
    institutionType: str
    accountStatus: str
    closedDate: int
    shareWorksAccount: bool
    fcManagedMssbClosedAccount: bool
