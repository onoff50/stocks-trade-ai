"""Async broker adapter wrapping the Groww SDK behind the rate limiter."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from growwapi import GrowwAPI
from growwapi.groww.exceptions import GrowwAPIRateLimitException

from .rate_limiter import RateLimiter

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PlaceResult:
    broker_order_id: str
    status: str


class Broker(Protocol):
    async def place_limit_sell(
        self, symbol: str, exchange: str, segment: str, product: str,
        qty: int, price: Decimal,
    ) -> PlaceResult: ...

    async def cancel(self, broker_order_id: str, segment: str) -> None: ...

    async def modify_limit_price(
        self, broker_order_id: str, segment: str, qty: int, price: Decimal,
    ) -> None: ...

    async def fetch_order(self, broker_order_id: str, segment: str) -> dict[str, Any]: ...


class GrowwBroker:
    """Real broker. The SDK is synchronous; we offload to a thread executor."""

    def __init__(self, api: GrowwAPI, limiter: RateLimiter) -> None:
        self._api = api
        self._limiter = limiter

    async def _call(self, fn, *args, **kwargs) -> Any:
        await self._limiter.acquire()
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except GrowwAPIRateLimitException:
            log.warning("broker returned 429; backing off 2s then retrying once")
            await asyncio.sleep(2.0)
            await self._limiter.acquire()
            return await asyncio.to_thread(fn, *args, **kwargs)

    async def place_limit_sell(
        self, symbol: str, exchange: str, segment: str, product: str,
        qty: int, price: Decimal,
    ) -> PlaceResult:
        resp = await self._call(
            self._api.place_order,
            validity=self._api.VALIDITY_DAY,
            exchange=exchange,
            order_type=self._api.ORDER_TYPE_LIMIT,
            product=product,
            quantity=qty,
            segment=segment,
            trading_symbol=symbol,
            transaction_type=self._api.TRANSACTION_TYPE_SELL,
            price=float(price),
        )
        return PlaceResult(
            broker_order_id=str(resp.get("groww_order_id") or resp.get("orderId") or resp.get("id")),
            status=str(resp.get("order_status") or resp.get("status") or "PENDING"),
        )

    async def cancel(self, broker_order_id: str, segment: str) -> None:
        await self._call(self._api.cancel_order, groww_order_id=broker_order_id, segment=segment)

    async def modify_limit_price(
        self, broker_order_id: str, segment: str, qty: int, price: Decimal,
    ) -> None:
        await self._call(
            self._api.modify_order,
            order_type=self._api.ORDER_TYPE_LIMIT,
            segment=segment,
            groww_order_id=broker_order_id,
            quantity=qty,
            price=float(price),
        )

    async def fetch_order(self, broker_order_id: str, segment: str) -> dict[str, Any]:
        return await self._call(self._api.get_order_detail, segment=segment, groww_order_id=broker_order_id)

    async def fetch_trades(self, broker_order_id: str, segment: str) -> list[dict[str, Any]]:
        resp = await self._call(
            self._api.get_trade_list_for_order, groww_order_id=broker_order_id, segment=segment,
        )
        trades = resp.get("trades") or resp.get("trade_list") or []
        return list(trades)


class FakeBroker:
    """In-memory broker for tests. Fills are driven by test code."""

    def __init__(self) -> None:
        self._next_id = 0
        self.orders: dict[str, dict[str, Any]] = {}

    def _new_id(self) -> str:
        self._next_id += 1
        return f"FAKE-{self._next_id}"

    async def place_limit_sell(
        self, symbol: str, exchange: str, segment: str, product: str,
        qty: int, price: Decimal,
    ) -> PlaceResult:
        oid = self._new_id()
        self.orders[oid] = {
            "symbol": symbol, "exchange": exchange, "segment": segment,
            "product": product, "qty": qty, "price": price, "status": "OPEN",
            "fills": [],
        }
        return PlaceResult(broker_order_id=oid, status="OPEN")

    async def cancel(self, broker_order_id: str, segment: str) -> None:
        if broker_order_id in self.orders:
            self.orders[broker_order_id]["status"] = "CANCELLED"

    async def modify_limit_price(
        self, broker_order_id: str, segment: str, qty: int, price: Decimal,
    ) -> None:
        if broker_order_id in self.orders:
            self.orders[broker_order_id]["price"] = price
            self.orders[broker_order_id]["qty"] = qty

    async def fetch_order(self, broker_order_id: str, segment: str) -> dict[str, Any]:
        return dict(self.orders.get(broker_order_id, {}))

    async def fetch_trades(self, broker_order_id: str, segment: str) -> list[dict[str, Any]]:
        return list(self.orders.get(broker_order_id, {}).get("fills", []))

    def fill(self, broker_order_id: str, qty: int, price: Decimal) -> None:
        """Test helper — simulate a fill."""
        o = self.orders[broker_order_id]
        o["fills"].append({"qty": qty, "price": price})
        filled = sum(f["qty"] for f in o["fills"])
        o["status"] = "FILLED" if filled >= o["qty"] else "PARTIALLY_FILLED"
