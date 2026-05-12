from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    NEW = "NEW"
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.FAILED}
)


@dataclass(frozen=True, slots=True)
class OHLCBar:
    """A single historical candle."""

    start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True, slots=True)
class Quote:
    """Top-of-book snapshot."""

    timestamp: datetime
    bid: Decimal
    ask: Decimal
    bid_qty: int
    ask_qty: int
    last_trade: Decimal | None = None
    last_trade_qty: int | None = None

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal(2)

    @property
    def spread_bps(self) -> Decimal:
        m = self.mid
        if m == 0:
            return Decimal(0)
        return (self.ask - self.bid) / m * Decimal(10_000)


@dataclass(frozen=True, slots=True)
class Bucket:
    """One slot in the VWAP schedule."""

    index: int
    start: datetime
    end: datetime
    planned_qty: int


@dataclass(slots=True)
class SlicePlan:
    buckets: list[Bucket]
    total_qty: int

    def bucket_for(self, ts: datetime) -> Bucket | None:
        for b in self.buckets:
            if b.start <= ts < b.end:
                return b
        return None

    @property
    def total_planned(self) -> int:
        return sum(b.planned_qty for b in self.buckets)


@dataclass(slots=True)
class Fill:
    """A single execution against a child order."""

    qty: int
    price: Decimal
    timestamp: datetime
    trade_id: str | None = None


@dataclass(slots=True)
class ChildOrder:
    local_id: str
    bucket_index: int
    side: Side
    qty: int
    price: Decimal | None  # None for market orders
    order_type: str  # "LIMIT" / "MARKET"
    status: OrderStatus = OrderStatus.NEW
    broker_order_id: str | None = None
    placed_at: datetime | None = None
    last_status_at: datetime | None = None
    fills: list[Fill] = field(default_factory=list)
    reject_reason: str | None = None

    @property
    def filled_qty(self) -> int:
        return sum(f.qty for f in self.fills)

    @property
    def remaining_qty(self) -> int:
        return max(0, self.qty - self.filled_qty)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def avg_fill_price(self) -> Decimal | None:
        if not self.fills:
            return None
        notional = sum((f.price * Decimal(f.qty) for f in self.fills), Decimal(0))
        return notional / Decimal(self.filled_qty)


@dataclass(frozen=True, slots=True)
class ParentOrder:
    """The user's top-level sell intent."""

    session_id: str
    symbol: str
    exchange: str
    segment: str
    product: str
    side: Side
    total_qty: int
    window_start: datetime
    window_end: datetime
    dry_run: bool
    arrival_mid: Decimal | None = None
