from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from stocks_trade_ai.config import IST, Settings
from stocks_trade_ai.dashboard.server import summarize_session
from stocks_trade_ai.scheduler import build_slice_plan
from stocks_trade_ai.state_store import StateStore
from stocks_trade_ai.types import ChildOrder, Fill, OrderStatus, ParentOrder, Side


async def _seed(settings: Settings, session_id: str) -> StateStore:
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    parent = ParentOrder(
        session_id=session_id, symbol="GROWW", exchange="NSE", segment="CASH",
        product="CNC", side=Side.SELL, total_qty=200,
        window_start=start, window_end=start + timedelta(hours=1),
        dry_run=True, arrival_mid=Decimal("100"),
    )
    store = StateStore(settings.state_dir / f"{session_id}.db")
    await store.open()
    await store.save_parent(parent, pid=4242, started_at=start)
    plan = build_slice_plan(200, {}, parent.window_start, parent.window_end)
    await store.save_slice_plan(session_id, plan)
    child = ChildOrder(
        local_id="c1", bucket_index=0, side=Side.SELL, qty=100,
        price=Decimal("99.50"), order_type="LIMIT", status=OrderStatus.FILLED,
        placed_at=start, last_status_at=start,
    )
    await store.upsert_child(session_id, child)
    await store.append_fill("c1", Fill(qty=100, price=Decimal("99.50"), timestamp=start))
    return store


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        groww_api_key="k", groww_totp_secret="t",
        state_dir=tmp_path, dashboard_bind="127.0.0.1:0",
        log_level="INFO", adv_cap_pct=10, per_child_pct_of_5min_volume=1,
        slippage_bps=30,
    )


async def test_summarize_session_shape(settings: Settings):
    store = await _seed(settings, "vwap-shape-1")
    try:
        data = await summarize_session(store, "vwap-shape-1")
    finally:
        await store.close()

    assert data is not None
    assert data["session_id"] == "vwap-shape-1"
    assert data["symbol"] == "GROWW"
    assert data["total_qty"] == 200
    assert data["filled_qty"] == 100
    assert data["filled_pct"] == 50.0
    assert data["dry_run"] is True
    assert data["arrival_mid"] == "100"
    assert data["avg_fill_price"] == 99.5
    # slippage_bps = (100 - 99.5) / 100 * 10000 = 50
    assert data["slippage_bps"] == pytest.approx(50.0)
    assert len(data["buckets"]) > 0
    assert len(data["children"]) == 1
    assert data["children"][0]["status"] == "FILLED"


async def test_summarize_session_missing(settings: Settings):
    store = StateStore(settings.state_dir / "missing.db")
    await store.open()
    try:
        data = await summarize_session(store, "no-such-session")
    finally:
        await store.close()
    assert data is None
