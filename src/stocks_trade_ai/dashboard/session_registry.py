"""Tracks dry-run sell sessions running inside the monitor process.

Each session gets its own GrowwMarketData (so a sell on RELIANCE keeps its
own feed even when the monitor view switches to a different symbol) and its
own StateStore (per-session SQLite db). The Engine runs as an asyncio task
owned by the registry; killing a session sets the engine's kill_event and
the task winds down cleanly.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from growwapi import GrowwAPI, GrowwFeed

from ..broker import Broker, GrowwBroker
from ..config import IST, Settings
from ..engine import Engine, new_session_id
from ..market_data import GrowwMarketData
from ..rate_limiter import RateLimiter
from ..state_store import StateStore
from ..types import ParentOrder, Side
from ..volume_profile import median_volume_profile

log = logging.getLogger(__name__)

FIRST_QUOTE_TIMEOUT_SEC = 10.0
FIRST_QUOTE_POLL_SEC = 0.5


@dataclass
class RunningSession:
    session_id: str
    parent: ParentOrder
    engine: Engine
    task: asyncio.Task[None]
    market_data: GrowwMarketData
    state_store: StateStore
    started_at: datetime
    ended_at: datetime | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class SellRequest:
    symbol: str
    qty: int
    window_start: datetime
    window_end: datetime
    exchange: str = "NSE"
    segment: str = "CASH"
    product: str = "CNC"
    allow_no_adv_cap: bool = True
    child_min_qty: int | None = None
    child_max_qty: int | None = None
    # When dry_run is False the engine routes through the real GrowwBroker.
    # The route handler must already have enforced the ALLOW_LIVE_TRADES env
    # gate before constructing a SellRequest with dry_run=False.
    dry_run: bool = True
    # Optional floor price: child orders are not placed when the best bid is
    # below this value. None disables the check.
    min_price: Decimal | None = None


class SessionRegistry:
    """Process-global registry of dry-run sell sessions.

    The registry only owns dry-run sessions for now — live trading is gated
    out at the CLI / form layer and not at this class.
    """

    def __init__(
        self, settings: Settings, api: GrowwAPI, feed: GrowwFeed,
        history_fetcher: Any,
    ) -> None:
        self._settings = settings
        self._api = api
        self._feed = feed
        # Callable[[api, parent], Awaitable[tuple[list[OHLCBar], float]]]
        # Injected so tests / refactors can swap the implementation.
        self._history_fetcher = history_fetcher
        self._sessions: dict[str, RunningSession] = {}
        self._limiter = RateLimiter(per_sec=8, per_min=200, name="orders")
        self._lock = asyncio.Lock()

    def list_sessions(self) -> list[RunningSession]:
        return list(self._sessions.values())

    def get(self, session_id: str) -> RunningSession | None:
        return self._sessions.get(session_id)

    async def start(self, req: SellRequest) -> RunningSession:
        """Validate, build, and spawn an engine task. Always dry-run.

        Raises KeyError on unknown symbol, ValueError on bad parameters.
        """
        from growwapi.groww.exceptions import InstrumentNotFoundException

        if req.qty <= 0:
            raise ValueError("qty must be > 0")
        if req.window_end <= req.window_start:
            raise ValueError("window_end must be after window_start")

        async with self._lock:
            md = GrowwMarketData(
                self._api, self._feed,
                exchange=req.exchange, trading_symbol=req.symbol.upper(),
                segment=req.segment,
            )
            try:
                await md.start()
            except InstrumentNotFoundException as exc:
                with _suppressed_aclose(md):
                    await md.aclose()
                raise KeyError(req.symbol.upper()) from exc
            except Exception:
                # md.start does network + subscription work — if it fails we
                # surface the error to the caller without leaving a half-open
                # session in the registry.
                with _suppressed_aclose(md):
                    await md.aclose()
                raise

            parent = ParentOrder(
                session_id=new_session_id(),
                symbol=req.symbol.upper(),
                exchange=req.exchange,
                segment=req.segment,
                product=req.product,
                side=Side.SELL,
                total_qty=req.qty,
                window_start=req.window_start,
                window_end=req.window_end,
                dry_run=req.dry_run,
                arrival_mid=None,
            )

            # Pull arrival_mid from the live feed (best effort, ~10s).
            parent = await _attach_arrival_mid(md, parent)
            bars, adv = await self._history_fetcher(self._api, parent)
            profile = median_volume_profile(bars)

            db_path = self._settings.state_dir / f"{parent.session_id}.db"
            state = StateStore(db_path)
            await state.open()

            broker: Broker = GrowwBroker(self._api, self._limiter)
            engine = Engine(
                settings=self._settings, broker=broker, market_data=md, state=state,
                parent=parent, adv_20day=adv, volume_profile=profile,
                allow_no_adv_cap=req.allow_no_adv_cap,
                child_min_qty=req.child_min_qty,
                child_max_qty=req.child_max_qty,
                min_price=req.min_price,
            )

            running = RunningSession(
                session_id=parent.session_id, parent=parent, engine=engine,
                task=asyncio.create_task(
                    self._supervise(engine, md, state, parent.session_id),
                    name=f"session-{parent.session_id}",
                ),
                market_data=md, state_store=state,
                started_at=datetime.now(tz=IST),
            )
            self._sessions[parent.session_id] = running
            mode = "DRY-RUN" if parent.dry_run else "*** LIVE ***"
            log.warning(
                "Started %s session %s: %s qty=%d window=%s..%s",
                mode, parent.session_id, parent.symbol, parent.total_qty,
                parent.window_start.isoformat(timespec="seconds"),
                parent.window_end.isoformat(timespec="seconds"),
            )
            return running

    async def kill(self, session_id: str) -> bool:
        sess = self._sessions.get(session_id)
        if sess is None:
            return False
        if sess.task.done():
            return True
        sess.engine.request_kill()
        return True

    async def shutdown(self) -> None:
        """Cancel all running tasks and close resources. Called at process exit."""
        for sess in list(self._sessions.values()):
            if not sess.task.done():
                sess.engine.request_kill()
        # Give engines a moment to wind down cleanly before cancelling.
        await asyncio.sleep(0.5)
        for sess in list(self._sessions.values()):
            if not sess.task.done():
                sess.task.cancel()
        for sess in list(self._sessions.values()):
            try:
                await sess.task
            except (asyncio.CancelledError, Exception):
                pass
            try:
                await sess.state_store.close()
            except Exception:
                pass

    async def _supervise(
        self, engine: Engine, md: GrowwMarketData, state: StateStore,
        session_id: str,
    ) -> None:
        try:
            await engine.run()
        except Exception as exc:  # noqa: BLE001
            log.error("session %s crashed: %s", session_id, exc)
            running = self._sessions.get(session_id)
            if running is not None:
                running.error = str(exc)
        finally:
            running = self._sessions.get(session_id)
            if running is not None:
                running.ended_at = datetime.now(tz=IST)
            with _suppressed_aclose(md):
                await md.aclose()
            # Note: we deliberately keep `state` open so /api/sessions can
            # summarize the completed session. It's closed in shutdown().


class _suppressed_aclose:
    """Context manager that swallows errors during async cleanup."""

    def __init__(self, _md: GrowwMarketData) -> None:
        pass

    def __enter__(self) -> "_suppressed_aclose":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return True  # always swallow


async def _attach_arrival_mid(md: GrowwMarketData, parent: ParentOrder) -> ParentOrder:
    waited = 0.0
    while waited < FIRST_QUOTE_TIMEOUT_SEC:
        q = await md.latest_quote()
        if q is not None:
            return dataclasses.replace(parent, arrival_mid=q.mid)
        await asyncio.sleep(FIRST_QUOTE_POLL_SEC)
        waited += FIRST_QUOTE_POLL_SEC
    log.warning(
        "No quote within %.0fs for %s — running without arrival_mid (slippage "
        "monitor disabled for this session).", FIRST_QUOTE_TIMEOUT_SEC, parent.symbol,
    )
    return parent
