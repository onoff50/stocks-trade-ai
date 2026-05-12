from datetime import datetime, time, timedelta

import pytest

from stocks_trade_ai.config import IST
from stocks_trade_ai.scheduler import build_slice_plan, rebalance_residual


def _window() -> tuple[datetime, datetime]:
    return (
        datetime(2026, 5, 12, 9, 15, tzinfo=IST),
        datetime(2026, 5, 12, 10, 0, tzinfo=IST),
    )


def test_slice_plan_sums_to_total_qty_uniform():
    start, end = _window()
    plan = build_slice_plan(1000, {}, start, end)
    assert plan.total_qty == 1000
    assert sum(b.planned_qty for b in plan.buckets) == 1000


def test_slice_plan_respects_volume_profile():
    start, end = _window()
    profile = {time(9, 15): 0.6, time(9, 20): 0.4}  # remaining buckets get 0
    plan = build_slice_plan(100, profile, start, end)
    assert plan.buckets[0].planned_qty == 60
    assert plan.buckets[1].planned_qty == 40
    assert all(b.planned_qty == 0 for b in plan.buckets[2:])


def test_slice_plan_handles_indivisible_quantities():
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    end = start + timedelta(minutes=15)  # 3 buckets
    plan = build_slice_plan(7, {}, start, end)
    qtys = sorted(b.planned_qty for b in plan.buckets)
    assert sum(qtys) == 7
    assert qtys == [2, 2, 3] or qtys == [2, 3, 2] or qtys == [3, 2, 2] or qtys == [2, 2, 3]


def test_slice_plan_rejects_nonpositive_qty():
    start, end = _window()
    with pytest.raises(ValueError):
        build_slice_plan(0, {}, start, end)


def test_rebalance_residual_under_fill():
    start, end = _window()
    plan = build_slice_plan(900, {}, start, end)  # 9 buckets, 100 each
    # After bucket 0 we only got 50 filled instead of 100.
    new_plan = rebalance_residual(plan, filled_through_index=0, actually_filled_qty=50)
    assert new_plan.total_qty == 900
    assert new_plan.buckets[0].planned_qty == 100  # past buckets are frozen history
    # Remaining 850 spread over 8 buckets: 106 or 107 each.
    future_qtys = [b.planned_qty for b in new_plan.buckets[1:]]
    assert sum(future_qtys) == 850
    assert all(q in (106, 107) for q in future_qtys)


def test_rebalance_residual_done_early():
    start, end = _window()
    plan = build_slice_plan(500, {}, start, end)
    # Already done after bucket 2.
    new_plan = rebalance_residual(plan, filled_through_index=2, actually_filled_qty=500)
    assert all(b.planned_qty == 0 for b in new_plan.buckets[3:])


def test_rebalance_residual_overfill():
    start, end = _window()
    plan = build_slice_plan(900, {}, start, end)
    new_plan = rebalance_residual(plan, filled_through_index=2, actually_filled_qty=600)
    # 300 left over 6 buckets = 50 each.
    future_qtys = [b.planned_qty for b in new_plan.buckets[3:]]
    assert sum(future_qtys) == 300
    assert all(q == 50 for q in future_qtys)
