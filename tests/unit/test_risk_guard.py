from decimal import Decimal

from stocks_trade_ai.risk_guard import (
    RiskState,
    SlippageMonitor,
    cap_child_qty,
    check_adv_cap,
)


def test_adv_cap_accepts_within_threshold():
    d = check_adv_cap(parent_qty=10_000, adv_20day=200_000, cap_pct=10)
    assert d.state == RiskState.OK


def test_adv_cap_rejects_oversize():
    d = check_adv_cap(parent_qty=50_000, adv_20day=200_000, cap_pct=10)
    assert d.state == RiskState.REJECTED
    assert d.reason and "exceeds" in d.reason


def test_adv_cap_rejects_when_adv_unknown():
    d = check_adv_cap(parent_qty=100, adv_20day=0, cap_pct=10)
    assert d.state == RiskState.REJECTED


def test_cap_child_qty_under_cap():
    assert cap_child_qty(planned_qty=50, recent_5min_volume=10_000, pct=1) == 50


def test_cap_child_qty_over_cap():
    # 1% of 10000 = 100, planned 500 → capped to 100.
    assert cap_child_qty(planned_qty=500, recent_5min_volume=10_000, pct=1) == 100


def test_cap_child_qty_minimum_one_when_volume_known():
    assert cap_child_qty(planned_qty=10, recent_5min_volume=50, pct=1) == 1


def test_slippage_monitor_within_threshold():
    m = SlippageMonitor(arrival_mid=Decimal(100), threshold_bps=30)
    tripped = m.observe_fill(qty=100, price=Decimal("99.95"))
    assert not tripped
    assert m.slippage_bps == 5


def test_slippage_monitor_trips_on_excess():
    m = SlippageMonitor(arrival_mid=Decimal(100), threshold_bps=30)
    # 50 bps drift → trips
    tripped = m.observe_fill(qty=100, price=Decimal("99.50"))
    assert tripped
    assert m.tripped


def test_slippage_monitor_does_not_double_trip():
    m = SlippageMonitor(arrival_mid=Decimal(100), threshold_bps=30)
    m.observe_fill(qty=100, price=Decimal("99.50"))
    again = m.observe_fill(qty=100, price=Decimal("99.0"))
    assert again is False


def test_slippage_monitor_resume():
    m = SlippageMonitor(arrival_mid=Decimal(100), threshold_bps=30)
    m.observe_fill(qty=100, price=Decimal("99.50"))
    m.reset_trip()
    assert not m.tripped
