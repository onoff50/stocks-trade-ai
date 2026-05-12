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
        self._latest_raw_depth: dict[str, Any] | None = None
        self._pending_order_updates: list[dict[str, Any]] = []
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self._switch_lock = asyncio.Lock()

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
                self._latest_raw_depth = depth_dict
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

    @property
    def latest_raw_depth(self) -> dict[str, Any] | None:
        """Sync snapshot of the most recent raw market depth dict (all tokens)."""
        return self._latest_raw_depth

    @property
    def trading_symbol(self) -> str:
        return self._symbol

    @property
    def exchange_token(self) -> str | None:
        if not self._instrument:
            return None
        return str(self._instrument.get("exchange_token"))

    @property
    def latest_payload(self) -> dict[str, Any] | None:
        """The depth payload for THIS instance's instrument only.

        Drills `{exchange: {segment: {token: payload}}}` and pulls the entry
        matching our exchange_token so multiple concurrent subscriptions
        (e.g. monitor + sell session for different symbols) don't cross-talk.
        """
        token = self.exchange_token
        raw = self._latest_raw_depth
        if not token or not isinstance(raw, dict):
            return None
        by_exch = raw.get(self._exchange)
        if not isinstance(by_exch, dict):
            return None
        by_seg = by_exch.get(self._segment)
        if not isinstance(by_seg, dict):
            return None
        payload = by_seg.get(token)
        return payload if isinstance(payload, dict) else None

    async def switch_symbol(self, new_symbol: str) -> dict[str, Any]:
        """Swap the active instrument: unsubscribe old, subscribe new.

        Returns the new instrument dict. Raises KeyError when the symbol is
        unknown so callers can render an inline error without falling over.
        """
        from growwapi.groww.exceptions import InstrumentNotFoundException

        new_symbol = new_symbol.upper().strip()
        async with self._switch_lock:
            if new_symbol == self._symbol and self._instrument is not None:
                return self._instrument
            try:
                new_instrument = await asyncio.to_thread(
                    self._api.get_instrument_by_exchange_and_trading_symbol,
                    self._exchange, new_symbol,
                )
            except InstrumentNotFoundException as exc:
                raise KeyError(new_symbol) from exc
            if not new_instrument or "exchange_token" not in new_instrument:
                raise KeyError(new_symbol)
            old_instrument = self._instrument
            if old_instrument is not None:
                try:
                    await asyncio.to_thread(
                        self._feed.unsubscribe_market_depth,
                        [{
                            "exchange": self._exchange,
                            "segment": self._segment,
                            "exchange_token": old_instrument["exchange_token"],
                        }],
                    )
                except Exception as exc:  # noqa: BLE001 — SDK errors here are non-fatal
                    log.warning("unsubscribe failed for %s: %s", self._symbol, exc)
            await asyncio.to_thread(
                self._feed.subscribe_market_depth,
                [{
                    "exchange": self._exchange,
                    "segment": self._segment,
                    "exchange_token": new_instrument["exchange_token"],
                }],
            )
            self._instrument = new_instrument
            self._symbol = new_symbol
            self._latest_quote = None
            self._latest_raw_depth = None
            log.info("Switched market data subscription to %s on %s",
                     new_symbol, self._exchange)
            return new_instrument

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

    Real shape from Groww: {exchange: {segment: {exchange_token: {tsInMillis,
    buyBook, sellBook}}}} where buyBook/sellBook are dicts keyed by "1".."N"
    level strings, with {price, qty}. We also tolerate flat shapes (legacy/test).
    """
    payload = _extract_depth_payload(depth_dict)
    if payload is None:
        return None
    bids = _sorted_levels(payload.get("buyBook") or payload.get("buy") or payload.get("bids"))
    asks = _sorted_levels(payload.get("sellBook") or payload.get("sell") or payload.get("asks"))
    if not bids or not asks:
        return None
    top_bid, top_ask = bids[0], asks[0]
    return Quote(
        timestamp=datetime.now(tz=IST),
        bid=Decimal(str(top_bid["price"])),
        ask=Decimal(str(top_ask["price"])),
        bid_qty=int(top_bid["qty"]),
        ask_qty=int(top_ask["qty"]),
        last_trade=Decimal(str(payload["ltp"])) if payload.get("ltp") is not None else None,
    )


def _extract_depth_payload(depth_dict: Any) -> dict[str, Any] | None:
    """Drill through nested {exchange:{segment:{token: payload}}} to find the
    first dict containing buyBook/sellBook (or buy/sell/bids/asks)."""
    if not isinstance(depth_dict, dict):
        return None
    if any(k in depth_dict for k in ("buyBook", "sellBook", "buy", "sell", "bids", "asks")):
        return depth_dict
    for v in depth_dict.values():
        inner = _extract_depth_payload(v)
        if inner is not None:
            return inner
    return None


def _sorted_levels(book: Any) -> list[dict[str, Any]]:
    """Normalize a depth book (dict keyed by '1'..'N', or already-a-list) into a
    list of {price, qty} dicts sorted by level index ascending (1 = best)."""
    if book is None:
        return []
    if isinstance(book, dict):
        try:
            items = sorted(book.items(), key=lambda kv: int(kv[0]))
        except (TypeError, ValueError):
            items = list(book.items())
        levels = [v for _, v in items if isinstance(v, dict)]
    elif isinstance(book, list):
        levels = [x for x in book if isinstance(x, dict)]
    else:
        return []
    out: list[dict[str, Any]] = []
    for lvl in levels:
        price = lvl.get("price")
        qty = lvl.get("qty", lvl.get("quantity", 0))
        if price is None:
            continue
        price_f = float(price)
        # Groww sometimes returns zero-priced filler levels (e.g. after market
        # close or during pre-open auction). Skip these — they aren't a real
        # book and would corrupt mid/spread calculations downstream.
        if price_f <= 0:
            continue
        out.append({"price": price_f, "qty": int(qty or 0)})
    return out


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
