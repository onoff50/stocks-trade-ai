from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from stocks_trade_ai.config import IST
from stocks_trade_ai.scheduler import build_slice_plan
from stocks_trade_ai.state_store import StateStore
from stocks_trade_ai.types import ChildOrder, Fill, OrderStatus, ParentOrder, Side


def _parent(session_id: str = "S1") -> ParentOrder:
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    return ParentOrder(
        session_id=session_id, symbol="RELIANCE", exchange="NSE", segment="CASH",
        product="CNC", side=Side.SELL, total_qty=1000,
        window_start=start, window_end=start + timedelta(hours=1),
        dry_run=False, arrival_mid=Decimal("1500.50"),
    )


@pytest.fixture
async def store(tmp_path: Path):
    s = StateStore(tmp_path / "state.db")
    await s.open()
    yield s
    await s.close()


async def test_save_and_load_parent(store: StateStore):
    p = _parent()
    await store.save_parent(p, pid=1234, started_at=datetime(2026, 5, 12, 9, 14, tzinfo=IST))
    loaded = await store.load_parent("S1")
    assert loaded is not None
    assert loaded.symbol == "RELIANCE"
    assert loaded.total_qty == 1000
    assert loaded.arrival_mid == Decimal("1500.50")
    assert loaded.side == Side.SELL
    assert loaded.dry_run is False


async def test_save_and_load_slice_plan(store: StateStore):
    p = _parent()
    await store.save_parent(p, pid=1, started_at=p.window_start)
    plan = build_slice_plan(p.total_qty, {}, p.window_start, p.window_end)
    await store.save_slice_plan(p.session_id, plan)
    loaded = await store.load_slice_plan(p.session_id)
    assert loaded is not None
    assert sum(b.planned_qty for b in loaded.buckets) == p.total_qty


async def test_upsert_child_and_fills(store: StateStore):
    p = _parent()
    await store.save_parent(p, pid=1, started_at=p.window_start)
    child = ChildOrder(
        local_id="c1", bucket_index=0, side=Side.SELL, qty=50,
        price=Decimal("1500"), order_type="LIMIT",
        status=OrderStatus.OPEN, broker_order_id="B1",
        placed_at=p.window_start, last_status_at=p.window_start,
    )
    await store.upsert_child(p.session_id, child)
    child.status = OrderStatus.PARTIALLY_FILLED
    child.fills.append(Fill(qty=20, price=Decimal("1500"), timestamp=p.window_start))
    await store.upsert_child(p.session_id, child)
    await store.append_fill("c1", child.fills[0])

    children = await store.load_children(p.session_id)
    assert len(children) == 1
    assert children[0].status == OrderStatus.PARTIALLY_FILLED
    assert children[0].filled_qty == 20
    assert children[0].fills[0].price == Decimal("1500")


async def test_completed_session_excluded_from_active(store: StateStore):
    p = _parent("S2")
    await store.save_parent(p, pid=1, started_at=p.window_start)
    actives = await store.list_active_sessions(p.window_start - timedelta(minutes=5))
    assert "S2" in actives
    await store.mark_completed("S2", p.window_end)
    actives = await store.list_active_sessions(p.window_start - timedelta(minutes=5))
    assert "S2" not in actives
