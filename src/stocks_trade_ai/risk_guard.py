"""Pre-trade caps and real-time slippage circuit-breaker."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

log = logging.getLogger(__name__)


class RiskState(StrEnum):
    OK = "OK"
    PAUSED = "PAUSED"  # slippage tripped; no new orders, user can resume
    REJECTED = "REJECTED"  # pre-trade reject; never started


@dataclass(frozen=True, slots=True)
class PreTradeDecision:
    state: RiskState
    reason: str | None = None


def check_adv_cap(parent_qty: int, adv_20day: float, cap_pct: float) -> PreTradeDecision:
    """Reject the parent if it's > cap_pct of the 20-day average daily volume."""
    if adv_20day <= 0:
        return PreTradeDecision(
            state=RiskState.REJECTED,
            reason="ADV unavailable; refusing to size-check the parent order",
        )
    threshold = adv_20day * cap_pct / 100
    if parent_qty > threshold:
        return PreTradeDecision(
            state=RiskState.REJECTED,
            reason=(
                f"parent_qty={parent_qty} exceeds {cap_pct:.1f}% of 20-day ADV "
                f"({adv_20day:.0f} → cap {threshold:.0f})"
            ),
        )
    return PreTradeDecision(state=RiskState.OK)


def cap_child_qty(planned_qty: int, recent_5min_volume: int, pct: float) -> int:
    """Cap a single child order at `pct` of the most recent 5-min market volume."""
    if recent_5min_volume <= 0:
        return planned_qty
    cap = int(recent_5min_volume * pct / 100)
    return min(planned_qty, max(1, cap))


class SlippageMonitor:
    """Tracks realized avg sell price drift vs. the parent's arrival mid.

    For a sell order, slippage is positive when we sell *below* arrival_mid.
    Trips when slippage exceeds `threshold_bps`.
    """

    def __init__(self, arrival_mid: Decimal, threshold_bps: float) -> None:
        if arrival_mid <= 0:
            raise ValueError("arrival_mid must be positive")
        self._arrival_mid = arrival_mid
        self._threshold = Decimal(str(threshold_bps))
        self._filled_qty = 0
        self._filled_notional = Decimal(0)
        self._tripped = False

    def observe_fill(self, qty: int, price: Decimal) -> bool:
        """Record a fill; return True iff this fill causes the breaker to trip."""
        if self._tripped:
            return False
        self._filled_qty += qty
        self._filled_notional += price * Decimal(qty)
        if self.slippage_bps > self._threshold:
            self._tripped = True
            log.warning(
                "slippage breaker tripped: realized=%s arrival_mid=%s slippage_bps=%s",
                self.avg_price,
                self._arrival_mid,
                self.slippage_bps,
            )
            return True
        return False

    @property
    def avg_price(self) -> Decimal | None:
        if self._filled_qty == 0:
            return None
        return self._filled_notional / Decimal(self._filled_qty)

    @property
    def slippage_bps(self) -> Decimal:
        avg = self.avg_price
        if avg is None:
            return Decimal(0)
        return (self._arrival_mid - avg) / self._arrival_mid * Decimal(10_000)

    @property
    def tripped(self) -> bool:
        return self._tripped

    def reset_trip(self) -> None:
        """User-initiated resume after reviewing a tripped session."""
        self._tripped = False
