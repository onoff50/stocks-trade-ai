from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from stocks_trade_ai.config import IST, Settings
from stocks_trade_ai.dashboard.server import create_app
from stocks_trade_ai.scheduler import build_slice_plan
from stocks_trade_ai.state_store import StateStore
from stocks_trade_ai.types import ChildOrder, Fill, OrderStatus, ParentOrder, Side


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        groww_api_key="k", groww_totp_secret="t",
        state_dir=tmp_path, dashboard_bind="127.0.0.1:0",
        log_level="INFO", adv_cap_pct=10, per_child_pct_of_5min_volume=1,
        slippage_bps=30,
    )


async def _seed(settings: Settings, session_id: str) -> None:
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    parent = ParentOrder(
        session_id=session_id, symbol="X", exchange="NSE", segment="CASH",
        product="CNC", side=Side.SELL, total_qty=100,
        window_start=start, window_end=start + timedelta(hours=1),
        dry_run=False, arrival_mid=Decimal("100"),
    )
    store = StateStore(settings.state_dir / f"{session_id}.db")
    await store.open()
    await store.save_parent(parent, pid=4242, started_at=start)
    plan = build_slice_plan(100, {}, parent.window_start, parent.window_end)
    await store.save_slice_plan(session_id, plan)
    child = ChildOrder(
        local_id="c1", bucket_index=0, side=Side.SELL, qty=50,
        price=Decimal("99.95"), order_type="LIMIT",
        status=OrderStatus.PARTIALLY_FILLED, broker_order_id="B1",
        placed_at=start, last_status_at=start,
    )
    fill = Fill(qty=20, price=Decimal("99.95"), timestamp=start)
    child.fills.append(fill)
    await store.upsert_child(session_id, child)
    await store.append_fill("c1", fill)
    await store.close()


async def test_state_endpoint_returns_session_progress(settings: Settings):
    await _seed(settings, "S-DASH")
    client = TestClient(create_app(settings, "S-DASH"))
    r = client.get("/state")
    assert r.status_code == 200
    s = r.json()
    assert s["symbol"] == "X"
    assert s["total_qty"] == 100
    assert s["filled_qty"] == 20
    assert len(s["children"]) == 1
    assert s["children"][0]["filled"] == 20
    assert s["buckets"][0]["filled"] == 20


def test_state_endpoint_404_for_unknown_session(settings: Settings):
    # Even an unknown session needs a DB file; create empty store.
    empty = settings.state_dir / "GHOST.db"
    empty.touch()
    client = TestClient(create_app(settings, "GHOST"))
    r = client.get("/state")
    assert r.status_code == 404


def test_index_page_renders(settings: Settings):
    empty = settings.state_dir / "Z.db"
    empty.touch()
    client = TestClient(create_app(settings, "Z"))
    r = client.get("/")
    assert r.status_code == 200
    assert "stocks-trade-ai" in r.text
    assert "Z" in r.text
