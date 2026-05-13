"""Within-bucket child order placement: passive limit → re-price → cross spread."""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from .broker import Broker
from .config import IST
from .market_data import MarketData
from .state_store import StateStore
from .types import (
    TERMINAL_STATUSES,
    Bucket,
    ChildOrder,
    Fill,
    OrderStatus,
    ParentOrder,
    Side,
)

log = logging.getLogger(__name__)

# Aggression schedule within a single bucket (fraction-of-elapsed → policy).
REPRICE_AT_FRAC = 0.60      # move from bid to mid
CROSS_SPREAD_AT_FRAC = 0.90  # actively take the ask, capped by max_cross_bps
DEFAULT_MAX_CROSS_BPS = Decimal("5")
DEFAULT_MAX_VISIBLE_QTY_PCT = Decimal("20")  # of bucket qty
DEFAULT_POLL_INTERVAL = 0.5  # seconds


class ChildOrderManager:
    """Drives a single bucket's qty to fill.

    For each "chunk" of the bucket's qty (visible slice), the manager places one
    LIMIT order at the policy-determined price, then waits for either (a) the
    fill completes the chunk, (b) the bucket elapses past a re-pricing threshold,
    or (c) the bucket ends and any open chunk is cancelled.
    """

    def __init__(
        self, *, broker: Broker, market_data: MarketData, state: StateStore,
        parent: ParentOrder,
        max_cross_bps: Decimal = DEFAULT_MAX_CROSS_BPS,
        max_visible_qty_pct: Decimal = DEFAULT_MAX_VISIBLE_QTY_PCT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        child_min_qty: int | None = None,
        child_max_qty: int | None = None,
        rng: random.Random | None = None,
        now: callable = lambda: datetime.now(tz=IST),
    ) -> None:
        self._broker = broker
        self._md = market_data
        self._state = state
        self._parent = parent
        self._max_cross_bps = max_cross_bps
        self._max_visible_qty_pct = max_visible_qty_pct
        self._poll_interval = poll_interval
        self._child_min = child_min_qty
        self._child_max = child_max_qty
        self._rng = rng or random.Random()
        self._now = now
        self._children: list[ChildOrder] = []

    def _pick_chunk_qty(self, remaining: int, chunk_max: int) -> int:
        """Pick the size for the next child order.

        - With [min, max] set: random in [min, min(max, remaining)], or just
          `remaining` when the bucket residual is below `min` (no further
          splitting possible — emit the tail).
        - Without bounds: the historical 20%-of-bucket behavior, clamped by
          the bucket's remaining qty.
        """
        if self._child_min is not None and self._child_max is not None:
            upper = min(self._child_max, remaining)
            if upper < self._child_min:
                return remaining
            return self._rng.randint(self._child_min, upper)
        return min(chunk_max, remaining)

    @property
    def children(self) -> list[ChildOrder]:
        return list(self._children)

    @property
    def bucket_filled_qty(self) -> int:
        return sum(c.filled_qty for c in self._children)

    async def run_bucket(self, bucket: Bucket) -> None:
        """Drive the bucket from its start to its end, placing/repricing/cancelling."""
        if bucket.planned_qty <= 0:
            return
        if self._parent.dry_run:
            await self._run_dry_run_bucket(bucket)
            return

        target = bucket.planned_qty
        chunk_max = max(1, int(target * self._max_visible_qty_pct / Decimal(100)))
        log.info(
            "bucket %d: target=%d chunk_max=%d window=[%s, %s]",
            bucket.index, target, chunk_max, bucket.start, bucket.end,
        )

        while self.bucket_filled_qty < target and self._now() < bucket.end:
            remaining = target - self.bucket_filled_qty
            chunk_qty = self._pick_chunk_qty(remaining, chunk_max)
            quote = await self._md.latest_quote()
            if quote is None:
                await asyncio.sleep(self._poll_interval)
                continue

            child = await self._place_passive_sell(bucket, chunk_qty, quote.bid)
            self._children.append(child)
            await self._drive_chunk_to_fill(child, bucket)

    async def _run_dry_run_bucket(self, bucket: Bucket) -> None:
        quote = await self._md.latest_quote()
        if quote is None:
            log.info("[DRY-RUN] bucket %d: no quote, would skip", bucket.index)
            return
        target = bucket.planned_qty
        chunk_max = max(1, int(target * self._max_visible_qty_pct / Decimal(100)))
        log.info(
            "[DRY-RUN] bucket %d: would sell %d @ bid %s "
            "(escalate to mid then cross by %s bps, child range=%s)",
            bucket.index, target, quote.bid, self._max_cross_bps,
            self._child_range_label(),
        )
        # Synthesize one filled child per chunk so the UI can show the
        # random-size jitter, not just a single all-in-one order.
        bucket_filled = 0
        while bucket_filled < target:
            remaining = target - bucket_filled
            chunk_qty = self._pick_chunk_qty(remaining, chunk_max)
            chunk_qty = max(1, min(chunk_qty, remaining))
            synthetic = ChildOrder(
                local_id=f"dry-{uuid.uuid4().hex[:8]}",
                bucket_index=bucket.index, side=Side.SELL,
                qty=chunk_qty, price=quote.bid, order_type="LIMIT",
                status=OrderStatus.FILLED,
                placed_at=self._now(), last_status_at=self._now(),
            )
            synthetic.fills.append(
                Fill(qty=chunk_qty, price=quote.bid, timestamp=self._now()),
            )
            self._children.append(synthetic)
            await self._state.upsert_child(self._parent.session_id, synthetic)
            await self._state.append_fill(synthetic.local_id, synthetic.fills[0])
            bucket_filled += chunk_qty

    def _child_range_label(self) -> str:
        if self._child_min is not None and self._child_max is not None:
            return f"[{self._child_min},{self._child_max}]"
        return f"~20%-of-bucket"

    async def _place_passive_sell(self, bucket: Bucket, qty: int, price: Decimal) -> ChildOrder:
        child = ChildOrder(
            local_id=f"c-{uuid.uuid4().hex[:8]}", bucket_index=bucket.index, side=Side.SELL,
            qty=qty, price=price, order_type="LIMIT", status=OrderStatus.NEW,
            placed_at=self._now(),
        )
        try:
            res = await self._broker.place_limit_sell(
                symbol=self._parent.symbol, exchange=self._parent.exchange,
                segment=self._parent.segment, product=self._parent.product,
                qty=qty, price=price,
            )
            child.broker_order_id = res.broker_order_id
            child.status = OrderStatus.OPEN
        except Exception as exc:
            log.error("place failed: %s", exc)
            child.status = OrderStatus.FAILED
            child.reject_reason = str(exc)
        child.last_status_at = self._now()
        await self._state.upsert_child(self._parent.session_id, child)
        return child

    async def _drive_chunk_to_fill(self, child: ChildOrder, bucket: Bucket) -> None:
        """Re-price / cross / cancel based on bucket elapsed fraction."""
        if child.status == OrderStatus.FAILED:
            return

        repriced_to_mid = False
        crossed = False
        bucket_dur = (bucket.end - bucket.start).total_seconds()

        while (
            child.status not in TERMINAL_STATUSES
            and child.remaining_qty > 0
            and self._now() < bucket.end
        ):
            await self._reconcile_child_status(child)
            if child.is_terminal or child.remaining_qty == 0:
                break

            elapsed_frac = (self._now() - bucket.start).total_seconds() / max(bucket_dur, 1)
            quote = await self._md.latest_quote()
            if quote is None:
                await asyncio.sleep(self._poll_interval)
                continue

            if not crossed and elapsed_frac >= CROSS_SPREAD_AT_FRAC:
                cap = quote.mid * (Decimal(1) + self._max_cross_bps / Decimal(10_000))
                new_price = min(quote.ask, cap)
                # For a sell we WANT to be at or below ask; lower price = more likely fill.
                # Cross by going to the ask itself (or as close as the cap allows).
                target_price = min(new_price, quote.ask)
                await self._safe_modify(child, target_price)
                crossed = True
            elif not repriced_to_mid and elapsed_frac >= REPRICE_AT_FRAC:
                await self._safe_modify(child, quote.mid)
                repriced_to_mid = True

            await asyncio.sleep(self._poll_interval)

        if child.status not in TERMINAL_STATUSES and child.remaining_qty > 0:
            # Bucket ended without full fill — cancel residual.
            await self._safe_cancel(child)

    async def _reconcile_child_status(self, child: ChildOrder) -> None:
        if not child.broker_order_id:
            return
        try:
            data = await self._broker.fetch_order(child.broker_order_id, self._parent.segment)
        except Exception as exc:
            log.warning("fetch_order failed for %s: %s", child.broker_order_id, exc)
            return

        new_status = _normalize_status(data)
        if new_status and new_status != child.status:
            child.status = new_status
            child.last_status_at = self._now()
            await self._state.upsert_child(self._parent.session_id, child)

        trades = await self._broker_safe_trades(child)
        await self._apply_new_fills(child, trades)

    async def _broker_safe_trades(self, child: ChildOrder) -> list[dict[str, Any]]:
        if not child.broker_order_id:
            return []
        try:
            return await self._broker.fetch_trades(child.broker_order_id, self._parent.segment)
        except Exception as exc:
            log.warning("fetch_trades failed: %s", exc)
            return []

    async def _apply_new_fills(self, child: ChildOrder, trades: list[dict[str, Any]]) -> None:
        already = {f.trade_id for f in child.fills if f.trade_id}
        for t in trades:
            tid = str(t.get("trade_id") or t.get("id") or "")
            if tid and tid in already:
                continue
            qty = int(t.get("qty") or t.get("quantity") or 0)
            price = Decimal(str(t.get("price") or t.get("fill_price") or 0))
            if qty <= 0 or price <= 0:
                continue
            fill = Fill(qty=qty, price=price, timestamp=self._now(), trade_id=tid or None)
            child.fills.append(fill)
            await self._state.append_fill(child.local_id, fill)
        if child.filled_qty >= child.qty:
            child.status = OrderStatus.FILLED
            await self._state.upsert_child(self._parent.session_id, child)

    async def _safe_modify(self, child: ChildOrder, new_price: Decimal) -> None:
        if not child.broker_order_id or child.is_terminal:
            return
        try:
            await self._broker.modify_limit_price(
                child.broker_order_id, self._parent.segment, child.remaining_qty, new_price,
            )
            child.price = new_price
            child.last_status_at = self._now()
            await self._state.upsert_child(self._parent.session_id, child)
            log.info("repriced %s -> %s", child.local_id, new_price)
        except Exception as exc:
            log.warning("modify failed for %s: %s", child.local_id, exc)

    async def _safe_cancel(self, child: ChildOrder) -> None:
        if not child.broker_order_id or child.is_terminal:
            return
        try:
            await self._broker.cancel(child.broker_order_id, self._parent.segment)
            child.status = OrderStatus.CANCELLED
            child.last_status_at = self._now()
            await self._state.upsert_child(self._parent.session_id, child)
        except Exception as exc:
            log.warning("cancel failed for %s: %s", child.local_id, exc)


_STATUS_ALIASES = {
    "OPEN": OrderStatus.OPEN, "NEW": OrderStatus.OPEN,
    "PENDING": OrderStatus.PENDING, "ACKNOWLEDGED": OrderStatus.OPEN,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED, "PART_FILLED": OrderStatus.PARTIALLY_FILLED,
    "COMPLETED": OrderStatus.FILLED, "FILLED": OrderStatus.FILLED, "EXECUTED": OrderStatus.FILLED,
    "CANCELLED": OrderStatus.CANCELLED, "CANCELED": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED, "FAILED": OrderStatus.FAILED,
}


def _normalize_status(data: dict[str, Any]) -> OrderStatus | None:
    raw = str(data.get("status") or data.get("order_status") or "").upper().strip()
    return _STATUS_ALIASES.get(raw)
