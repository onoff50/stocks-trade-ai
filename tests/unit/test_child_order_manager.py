from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from stocks_trade_ai.broker import FakeBroker
from stocks_trade_ai.child_order_manager import ChildOrderManager
from stocks_trade_ai.config import IST
from stocks_trade_ai.market_data import FakeMarketData
from stocks_trade_ai.state_store import StateStore
from stocks_trade_ai.types import Bucket, OrderStatus, ParentOrder, Quote, Side


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.t = start

    def __call__(self) -> datetime:
        return self.t

    def advance(self, secs: float) -> None:
        self.t = self.t + timedelta(seconds=secs)


@pytest.fixture
async def state(tmp_path: Path):
    s = StateStore(tmp_path / "s.db")
    await s.open()
    yield s
    await s.close()


def _parent(dry_run: bool = False) -> ParentOrder:
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    return ParentOrder(
        session_id="T1", symbol="X", exchange="NSE", segment="CASH",
        product="CNC", side=Side.SELL, total_qty=100,
        window_start=start, window_end=start + timedelta(minutes=30),
        dry_run=dry_run, arrival_mid=Decimal("100"),
    )


def _bucket() -> Bucket:
    p = _parent()
    return Bucket(index=0, start=p.window_start, end=p.window_start + timedelta(minutes=5), planned_qty=50)


def _quote(bid: Decimal = Decimal("99.95"), ask: Decimal = Decimal("100.05")) -> Quote:
    return Quote(
        timestamp=datetime.now(tz=IST), bid=bid, ask=ask, bid_qty=1000, ask_qty=1000,
    )


async def test_dry_run_fills_synthetically(state: StateStore):
    p = _parent(dry_run=True)
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    fb = FakeBroker()
    mgr = ChildOrderManager(broker=fb, market_data=md, state=state, parent=p)
    await mgr.run_bucket(_bucket())
    assert mgr.bucket_filled_qty == 50
    assert mgr.children[0].status == OrderStatus.FILLED
    # No real broker orders placed.
    assert fb.orders == {}


async def test_zero_qty_bucket_is_noop(state: StateStore):
    p = _parent()
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    fb = FakeBroker()
    mgr = ChildOrderManager(broker=fb, market_data=md, state=state, parent=p)
    b = Bucket(index=0, start=p.window_start, end=p.window_start + timedelta(minutes=5), planned_qty=0)
    await mgr.run_bucket(b)
    assert mgr.bucket_filled_qty == 0
    assert fb.orders == {}


async def test_immediate_fill_passive_path(state: StateStore):
    p = _parent()
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    fb = FakeBroker()

    clock = FakeClock(p.window_start)

    # Patch broker.place to auto-fill (simulating immediate execution).
    orig_place = fb.place_limit_sell

    async def auto_fill_place(**kw):
        r = await orig_place(**kw)
        fb.fill(r.broker_order_id, kw["qty"], kw["price"])
        return r

    fb.place_limit_sell = auto_fill_place  # type: ignore[method-assign]

    mgr = ChildOrderManager(
        broker=fb, market_data=md, state=state, parent=p,
        poll_interval=0.0, now=clock,
    )
    await mgr.run_bucket(_bucket())
    assert mgr.bucket_filled_qty == 50
    assert all(c.status == OrderStatus.FILLED for c in mgr.children)


async def test_unfilled_chunk_cancelled_at_bucket_end(state: StateStore):
    p = _parent()
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    fb = FakeBroker()
    bucket = _bucket()

    # Clock that jumps past bucket end after the first place.
    clock = FakeClock(bucket.start)
    place_count = 0
    orig_place = fb.place_limit_sell

    async def slow_place(**kw):
        nonlocal place_count
        place_count += 1
        r = await orig_place(**kw)
        clock.advance(10 * 60)  # 10 minutes elapse — past 5-min bucket end
        return r

    fb.place_limit_sell = slow_place  # type: ignore[method-assign]

    mgr = ChildOrderManager(
        broker=fb, market_data=md, state=state, parent=p,
        poll_interval=0.0, now=clock,
    )
    await mgr.run_bucket(bucket)

    assert place_count == 1  # only one chunk placed before bucket expired
    assert mgr.children[0].status == OrderStatus.CANCELLED
