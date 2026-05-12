"""Wraps GrowwFeed for L1 market depth + order updates, bridged to asyncio."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from growwapi import GrowwAPI, GrowwFeed

from .config import IST
from .types import Quote

log = logging.getLogger(__name__)


class MarketData(Protocol):
    async def latest_quote(self) -> Quote | None: ...
    async def latest_order_updates(self) -> list[dict[str, Any]]: ...
    async def aclose(self) -> None: ...


class GrowwMarketData:
    """Subscribes to a single equity instrument's depth + the user's order updates.

    The SDK uses a synchronous polling model: subscribe with a callback (we don't
    use it), then call `get_market_depth()` / `get_equity_order_update()` to pull
    the latest cached snapshot. We poll those in a background task and surface
    typed values to async consumers.
    """

    def __init__(
        self, api: GrowwAPI, feed: GrowwFeed,
        *, exchange: str, trading_symbol: str, segment: str,
        poll_interval_sec: float = 0.5,
    ) -> None:
        self._api = api
        self._feed = feed
        self._exchange = exchange
        self._symbol = trading_symbol
        self._segment = segment
        self._poll_interval = poll_interval_sec
        self._instrument: dict[str, Any] | None = None
        self._latest_quote: Quote | None = None
        self._pending_order_updates: list[dict[str, Any]] = []
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        self._instrument = await asyncio.to_thread(
            self._api.get_instrument_by_exchange_and_trading_symbol,
            self._exchange, self._symbol,
        )
        instrument_list = [{
            "exchange": self._exchange,
            "segment": self._segment,
            "exchange_token": self._instrument["exchange_token"],
        }]
        await asyncio.to_thread(self._feed.subscribe_market_depth, instrument_list)
        await asyncio.to_thread(self._feed.subscribe_equity_order_updates)
        self._task = asyncio.create_task(self._poll_loop(), name="market-data-poll")
        log.info("Subscribed market depth + order updates for %s on %s", self._symbol, self._exchange)

    async def _poll_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                depth_dict = await asyncio.to_thread(self._feed.get_market_depth)
                self._latest_quote = _parse_depth_to_quote(depth_dict, self._instrument)
                update = await asyncio.to_thread(self._feed.get_equity_order_update)
                if update:
                    self._pending_order_updates.append(update)
            except Exception as exc:
                log.warning("market data poll error: %s", exc)
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)
            except TimeoutError:
                pass

    async def latest_quote(self) -> Quote | None:
        return self._latest_quote

    async def latest_order_updates(self) -> list[dict[str, Any]]:
        out, self._pending_order_updates = self._pending_order_updates, []
        return out

    async def aclose(self) -> None:
        self._stopped.set()
        if self._task:
            try:
                await self._task
            except Exception:
                pass


def _parse_depth_to_quote(depth_dict: Any, instrument: Any) -> Quote | None:
    """Map a GrowwFeed market-depth payload to our Quote type.

    The exact shape is determined at runtime; we look for plausible field names
    so the same code handles snake_case and camelCase responses without coupling
    to a particular SDK release.
    """
    if not depth_dict:
        return None
    # The dict is keyed by exchange_token or topic; pull the first non-empty entry.
    payload: Any = depth_dict
    if isinstance(depth_dict, dict):
        for v in depth_dict.values():
            if isinstance(v, dict) and v:
                payload = v
                break
    bids = payload.get("buy") or payload.get("bids") or []
    asks = payload.get("sell") or payload.get("asks") or []
    if not bids or not asks:
        return None
    top_bid = bids[0]
    top_ask = asks[0]
    return Quote(
        timestamp=datetime.now(tz=IST),
        bid=Decimal(str(top_bid.get("price"))),
        ask=Decimal(str(top_ask.get("price"))),
        bid_qty=int(top_bid.get("quantity", 0)),
        ask_qty=int(top_ask.get("quantity", 0)),
        last_trade=Decimal(str(payload["ltp"])) if "ltp" in payload else None,
    )


class FakeMarketData:
    """In-memory market data feed for tests; drive with `push_quote`."""

    def __init__(self) -> None:
        self._quote: Quote | None = None
        self._order_updates: list[dict[str, Any]] = []

    def push_quote(self, q: Quote) -> None:
        self._quote = q

    def push_order_update(self, payload: dict[str, Any]) -> None:
        self._order_updates.append(payload)

    async def latest_quote(self) -> Quote | None:
        return self._quote

    async def latest_order_updates(self) -> list[dict[str, Any]]:
        out, self._order_updates = self._order_updates, []
        return out

    async def aclose(self) -> None:
        return None
