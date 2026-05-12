from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from stocks_trade_ai.broker import FakeBroker
from stocks_trade_ai.config import IST, Settings
from stocks_trade_ai.engine import Engine, EngineError
from stocks_trade_ai.market_data import FakeMarketData
from stocks_trade_ai.risk_guard import RiskState
from stocks_trade_ai.state_store import StateStore
from stocks_trade_ai.types import ParentOrder, Quote, Side


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        groww_api_key="k", groww_totp_secret="t",
        state_dir=tmp_path, dashboard_bind="127.0.0.1:0",
        log_level="INFO", adv_cap_pct=10, per_child_pct_of_5min_volume=1,
        slippage_bps=30,
    )


def _parent(window_start: datetime, qty: int = 100) -> ParentOrder:
    return ParentOrder(
        session_id="vwap-test", symbol="X", exchange="NSE", segment="CASH",
        product="CNC", side=Side.SELL, total_qty=qty,
        window_start=window_start, window_end=window_start + timedelta(minutes=10),
        dry_run=True, arrival_mid=None,
    )


def _build_engine(settings: Settings, *, adv_20day: float, allow_no_adv_cap: bool):
    parent = _parent(datetime.now(tz=IST))
    return Engine(
        settings=settings, broker=FakeBroker(), market_data=FakeMarketData(),
        state=StateStore(settings.state_dir / "x.db"),
        parent=parent, adv_20day=adv_20day, volume_profile={},
        allow_no_adv_cap=allow_no_adv_cap,
    )


def test_pre_trade_rejects_when_adv_unknown_and_flag_off(settings: Settings):
    engine = _build_engine(settings, adv_20day=0.0, allow_no_adv_cap=False)
    decision = engine._pre_trade()
    assert decision.state == RiskState.REJECTED


def test_pre_trade_ok_when_adv_unknown_and_flag_on(settings: Settings):
    engine = _build_engine(settings, adv_20day=0.0, allow_no_adv_cap=True)
    decision = engine._pre_trade()
    assert decision.state == RiskState.OK


def test_pre_trade_still_caps_oversize_when_flag_on(settings: Settings):
    """Flag should only matter when ADV is unknown — real ADV still enforced."""
    parent = _parent(datetime.now(tz=IST), qty=10_000_000)
    engine = Engine(
        settings=settings, broker=FakeBroker(), market_data=FakeMarketData(),
        state=StateStore(settings.state_dir / "x.db"),
        parent=parent, adv_20day=200_000.0, volume_profile={},
        allow_no_adv_cap=True,
    )
    decision = engine._pre_trade()
    assert decision.state == RiskState.REJECTED
