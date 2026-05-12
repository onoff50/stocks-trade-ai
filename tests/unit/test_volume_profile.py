from datetime import datetime, time, timedelta
from decimal import Decimal

import pytest

from stocks_trade_ai.config import IST
from stocks_trade_ai.types import OHLCBar
from stocks_trade_ai.volume_profile import (
    MIN_DAYS_FOR_PROFILE,
    build_schedule_weights,
    median_volume_profile,
)


def _bar(start: datetime, volume: int) -> OHLCBar:
    return OHLCBar(
        start=start, open=Decimal(100), high=Decimal(100), low=Decimal(100),
        close=Decimal(100), volume=volume,
    )


def _make_day(d: datetime, volumes: list[int]) -> list[OHLCBar]:
    return [
        _bar(d.replace(hour=9, minute=15) + timedelta(minutes=5 * i), v)
        for i, v in enumerate(volumes)
    ]


def test_empty_input_returns_empty():
    assert median_volume_profile([]) == {}


def test_too_few_days_returns_empty():
    bars: list[OHLCBar] = []
    for i in range(MIN_DAYS_FOR_PROFILE - 1):
        d = datetime(2026, 1, 5 + i, tzinfo=IST)
        bars += _make_day(d, [100, 200, 300])
    assert median_volume_profile(bars) == {}


def test_median_profile_shape():
    # 10 identical days: each bucket's share should equal that bucket's day-fraction.
    bars: list[OHLCBar] = []
    daily = [100, 200, 300, 400]  # totals to 1000
    expected_shares = {0.1, 0.2, 0.3, 0.4}
    for i in range(MIN_DAYS_FOR_PROFILE):
        d = datetime(2026, 1, 5 + i, tzinfo=IST)
        bars += _make_day(d, daily)
    profile = median_volume_profile(bars)
    assert len(profile) == 4
    assert set(round(v, 4) for v in profile.values()) == expected_shares
    assert sum(profile.values()) == pytest.approx(1.0)


def test_build_schedule_weights_uniform_when_profile_empty():
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    end = datetime(2026, 5, 12, 10, 0, tzinfo=IST)
    buckets = build_schedule_weights({}, start, end)
    assert len(buckets) == 9
    weights = [w for _, _, w in buckets]
    assert all(abs(w - 1 / 9) < 1e-9 for w in weights)


def test_build_schedule_weights_respects_profile():
    profile = {time(9, 15): 0.5, time(9, 20): 0.3, time(9, 25): 0.2}
    start = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    end = datetime(2026, 5, 12, 9, 30, tzinfo=IST)
    buckets = build_schedule_weights(profile, start, end)
    assert len(buckets) == 3
    weights = [round(w, 4) for _, _, w in buckets]
    assert weights == [0.5, 0.3, 0.2]


def test_build_schedule_weights_rejects_naive_datetimes():
    with pytest.raises(ValueError):
        build_schedule_weights({}, datetime(2026, 1, 1, 9, 15), datetime(2026, 1, 1, 10))


def test_build_schedule_weights_rejects_inverted_window():
    start = datetime(2026, 5, 12, 10, 0, tzinfo=IST)
    end = datetime(2026, 5, 12, 9, 15, tzinfo=IST)
    with pytest.raises(ValueError):
        build_schedule_weights({}, start, end)
