"""Top-level async loop that composes the engine for a single trading session."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from .broker import Broker
from .child_order_manager import ChildOrderManager
from .config import IST, Settings
from .market_data import MarketData
from .risk_guard import (
    PreTradeDecision,
    RiskState,
    SlippageMonitor,
    cap_child_qty,
    check_adv_cap,
)
from .scheduler import build_slice_plan, rebalance_residual
from .state_store import StateStore
from .types import ParentOrder, SlicePlan

log = logging.getLogger(__name__)


class EngineError(RuntimeError):
    pass


class Engine:
    def __init__(
        self, *, settings: Settings, broker: Broker, market_data: MarketData,
        state: StateStore, parent: ParentOrder,
        adv_20day: float, volume_profile: dict[Any, float],
    ) -> None:
        self._settings = settings
        self._broker = broker
        self._md = market_data
        self._state = state
        self._parent = parent
        self._adv_20day = adv_20day
        self._profile = volume_profile
        self._plan: SlicePlan | None = None
        self._slippage: SlippageMonitor | None = None
        self._kill_event = asyncio.Event()
        self._completed = False
        self._observed_fills: dict[str, int] = {}

    @property
    def plan(self) -> SlicePlan | None:
        return self._plan

    @property
    def slippage_bps(self) -> Decimal:
        return self._slippage.slippage_bps if self._slippage else Decimal(0)

    def request_kill(self) -> None:
        log.warning("kill requested")
        self._kill_event.set()

    async def run(self) -> None:
        """Execute the session: pre-trade checks → schedule → bucket loop → completion."""
        decision = self._pre_trade()
        if decision.state == RiskState.REJECTED:
            await self._state.log_event(
                self._parent.session_id, datetime.now(tz=IST),
                "pre_trade_reject", {"reason": decision.reason},
            )
            raise EngineError(f"pre-trade rejected: {decision.reason}")

        await self._initialize()
        try:
            await self._loop()
        finally:
            await self._on_exit()

    def _pre_trade(self) -> PreTradeDecision:
        return check_adv_cap(
            self._parent.total_qty, self._adv_20day, self._settings.adv_cap_pct,
        )

    async def _initialize(self) -> None:
        self._plan = build_slice_plan(
            self._parent.total_qty, self._profile,
            self._parent.window_start, self._parent.window_end,
        )
        await self._state.save_parent(
            self._parent, pid=os.getpid(), started_at=datetime.now(tz=IST),
        )
        await self._state.save_slice_plan(self._parent.session_id, self._plan)
        if self._parent.arrival_mid:
            self._slippage = SlippageMonitor(
                self._parent.arrival_mid, self._settings.slippage_bps,
            )
        await self._state.log_event(
            self._parent.session_id, datetime.now(tz=IST), "started",
            {"buckets": len(self._plan.buckets), "dry_run": self._parent.dry_run},
        )

    async def _loop(self) -> None:
        assert self._plan is not None
        mgr = ChildOrderManager(
            broker=self._broker, market_data=self._md, state=self._state, parent=self._parent,
        )
        total_filled = 0
        for i, bucket in enumerate(self._plan.buckets):
            if self._kill_event.is_set():
                log.warning("kill before bucket %d; stopping", i)
                break
            # Skip buckets whose end is already past (resume from crash).
            if datetime.now(tz=IST) >= bucket.end:
                continue
            # If the slippage breaker tripped, pause until cleared (or killed).
            await self._wait_if_paused()

            # Apply per-child cap based on the most recent 5-min volume estimate.
            recent_vol = await self._estimate_recent_5min_volume()
            capped = cap_child_qty(
                bucket.planned_qty, recent_vol, self._settings.per_child_pct_of_5min_volume,
            )
            if capped != bucket.planned_qty:
                log.info(
                    "bucket %d capped %d→%d (1%% of recent 5min vol=%d)",
                    bucket.index, bucket.planned_qty, capped, recent_vol,
                )
                bucket = bucket.__class__(  # immutable dataclass; clone with capped qty
                    index=bucket.index, start=bucket.start, end=bucket.end, planned_qty=capped,
                )

            await mgr.run_bucket(bucket)

            bucket_filled = sum(c.filled_qty for c in mgr.children if c.bucket_index == bucket.index)
            total_filled += bucket_filled
            await self._update_slippage(mgr)
            self._plan = rebalance_residual(self._plan, i, total_filled)
            await self._state.save_slice_plan(self._parent.session_id, self._plan)
            if total_filled >= self._parent.total_qty:
                log.info("parent qty fully filled, exiting bucket loop early")
                break

        self._completed = True

    async def _wait_if_paused(self) -> None:
        if not self._slippage or not self._slippage.tripped:
            return
        log.warning("slippage breaker tripped; pausing until resume or kill")
        while self._slippage.tripped and not self._kill_event.is_set():
            try:
                await asyncio.wait_for(self._kill_event.wait(), timeout=2.0)
            except TimeoutError:
                continue

    async def _update_slippage(self, mgr: ChildOrderManager) -> None:
        """Feed any not-yet-observed fills into the slippage monitor.

        We track which fills we've already pushed by (child_local_id, fill_index)
        so the monitor sees each fill exactly once.
        """
        if not self._slippage or not self._parent.arrival_mid:
            return
        for child in mgr.children:
            seen = self._observed_fills.setdefault(child.local_id, 0)
            for f in child.fills[seen:]:
                self._slippage.observe_fill(f.qty, f.price)
            self._observed_fills[child.local_id] = len(child.fills)

        avg = self._slippage.avg_price
        if avg is None:
            return
        await self._state.append_slippage_mark(
            self._parent.session_id, datetime.now(tz=IST),
            avg, self._parent.arrival_mid, self._slippage.slippage_bps,
        )

    async def _estimate_recent_5min_volume(self) -> int:
        # Placeholder: derive from the last few quote ticks. Without a tick-trade
        # aggregator, fall back to a large number so the cap doesn't bind.
        return 10_000_000

    async def _on_exit(self) -> None:
        if self._completed:
            await self._state.mark_completed(self._parent.session_id, datetime.now(tz=IST))
            await self._state.log_event(
                self._parent.session_id, datetime.now(tz=IST), "completed", {},
            )
        else:
            await self._state.log_event(
                self._parent.session_id, datetime.now(tz=IST), "interrupted", {},
            )


def install_signal_handlers(engine: Engine) -> None:
    loop = asyncio.get_running_loop()

    def _signal() -> None:
        engine.request_kill()

    with contextlib.suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _signal)
        loop.add_signal_handler(signal.SIGTERM, _signal)


def new_session_id() -> str:
    return f"vwap-{datetime.now(tz=IST).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
