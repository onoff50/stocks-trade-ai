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


# ---------- random child-size jitter ---------------------------------------

import random


async def test_child_chunks_random_within_bounds(state: StateStore):
    """With child_min=5 / child_max=15, every emitted child is in [5, 15]
    (or the tail residual when bucket remaining is below min), and they sum
    to the bucket planned qty."""
    p = _parent(dry_run=True)
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    rng = random.Random(42)
    mgr = ChildOrderManager(
        broker=FakeBroker(), market_data=md, state=state, parent=p,
        child_min_qty=5, child_max_qty=15, rng=rng,
    )
    b = Bucket(index=0, start=p.window_start, end=p.window_start + timedelta(minutes=5), planned_qty=100)
    await mgr.run_bucket(b)
    assert mgr.bucket_filled_qty == 100
    chunks = [c.qty for c in mgr.children]
    assert sum(chunks) == 100
    # All but possibly the last must respect [5,15]. The last can be a residual
    # tail if remaining < 5.
    for q in chunks[:-1]:
        assert 5 <= q <= 15, f"chunk {q} outside [5,15]"
    # Last chunk: in-range OR a tail residual (< min).
    assert chunks[-1] >= 1
    assert chunks[-1] <= 15
    # >1 chunk produced (otherwise the jitter test doesn't really exercise the loop).
    assert len(chunks) > 1


async def test_child_residual_falls_below_min(state: StateStore):
    """Bucket of 20 with min=15/max=18 — first chunk in [15,18], then the
    residual (< min) is emitted as a single tail chunk."""
    p = _parent(dry_run=True)
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    rng = random.Random(0)
    mgr = ChildOrderManager(
        broker=FakeBroker(), market_data=md, state=state, parent=p,
        child_min_qty=15, child_max_qty=18, rng=rng,
    )
    b = Bucket(index=0, start=p.window_start, end=p.window_start + timedelta(minutes=5), planned_qty=20)
    await mgr.run_bucket(b)
    chunks = [c.qty for c in mgr.children]
    assert sum(chunks) == 20
    assert 15 <= chunks[0] <= 18
    # The remainder is below `min` → emitted as a tail residual.
    assert chunks[-1] == 20 - chunks[0]


async def test_no_bounds_falls_back_to_20pct(state: StateStore):
    """With child_min/max=None, behavior matches the pre-jitter design:
    every chunk (except possibly the last) is 20% of bucket qty."""
    p = _parent(dry_run=True)
    await state.save_parent(p, pid=1, started_at=p.window_start)
    md = FakeMarketData()
    md.push_quote(_quote())
    mgr = ChildOrderManager(
        broker=FakeBroker(), market_data=md, state=state, parent=p,
    )
    b = Bucket(index=0, start=p.window_start, end=p.window_start + timedelta(minutes=5), planned_qty=100)
    await mgr.run_bucket(b)
    chunks = [c.qty for c in mgr.children]
    assert sum(chunks) == 100
    # 20% of 100 = 20, expect 5 chunks of 20.
    assert chunks == [20, 20, 20, 20, 20]


async def test_pick_chunk_qty_helper_bounds_and_fallback(state: StateStore):
    """Direct unit test of _pick_chunk_qty for the three code paths."""
    p = _parent(dry_run=True)
    mgr = ChildOrderManager(
        broker=FakeBroker(), market_data=FakeMarketData(), state=state, parent=p,
        child_min_qty=5, child_max_qty=10, rng=random.Random(1),
    )
    # In-range pick.
    q = mgr._pick_chunk_qty(remaining=100, chunk_max=20)
    assert 5 <= q <= 10
    # Remaining below min → residual.
    assert mgr._pick_chunk_qty(remaining=3, chunk_max=20) == 3
    # Remaining == min → exactly min.
    assert mgr._pick_chunk_qty(remaining=5, chunk_max=20) == 5
    # Without bounds → fallback to chunk_max.
    mgr2 = ChildOrderManager(
        broker=FakeBroker(), market_data=FakeMarketData(), state=state, parent=p,
    )
    assert mgr2._pick_chunk_qty(remaining=100, chunk_max=20) == 20
    assert mgr2._pick_chunk_qty(remaining=10, chunk_max=20) == 10
