"""Build a per-bucket volume distribution from historical 5-minute bars."""
from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import date, datetime, time, timedelta

from .config import IST
from .types import OHLCBar

log = logging.getLogger(__name__)

BUCKET_MINUTES = 5
MIN_DAYS_FOR_PROFILE = 10


def _bucket_key(ts: datetime) -> time:
    """Map a timestamp to its 5-min bucket start (time-of-day, IST)."""
    ts = ts.astimezone(IST)
    minute = (ts.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    return time(hour=ts.hour, minute=minute)


def median_volume_profile(bars: list[OHLCBar]) -> dict[time, float]:
    """Per-bucket median share-of-day volume across the input days.

    Each day is normalized to sum to 1.0 (so days with different total volume
    contribute equally to the median). Cold-start (insufficient days) returns
    an empty dict — caller should fall back to uniform.
    """
    if not bars:
        return {}

    by_day: dict[date, dict[time, int]] = defaultdict(lambda: defaultdict(int))
    for b in bars:
        d = b.start.astimezone(IST).date()
        by_day[d][_bucket_key(b.start)] += b.volume

    if len(by_day) < MIN_DAYS_FOR_PROFILE:
        log.warning(
            "Only %d days of history (< %d) — caller should use uniform fallback",
            len(by_day),
            MIN_DAYS_FOR_PROFILE,
        )
        return {}

    shares_by_bucket: dict[time, list[float]] = defaultdict(list)
    for buckets in by_day.values():
        day_total = sum(buckets.values())
        if day_total == 0:
            continue
        for bkt, vol in buckets.items():
            shares_by_bucket[bkt].append(vol / day_total)

    return {bkt: statistics.median(shares) for bkt, shares in shares_by_bucket.items()}


def build_schedule_weights(
    profile: dict[time, float],
    window_start: datetime,
    window_end: datetime,
) -> list[tuple[datetime, datetime, float]]:
    """Render the profile onto the user's execution window as (start,end,weight) buckets.

    Weights are renormalized to sum to 1.0 across the window. If the profile is
    empty (cold-start) the weights are uniform.
    """
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("window_start and window_end must be timezone-aware")
    if window_end <= window_start:
        raise ValueError("window_end must be after window_start")

    buckets: list[tuple[datetime, datetime, float]] = []
    step = timedelta(minutes=BUCKET_MINUTES)
    cursor = _floor_to_bucket(window_start)
    while cursor < window_end:
        end = min(cursor + step, window_end)
        weight = profile.get(_bucket_key(cursor), 0.0) if profile else 1.0
        buckets.append((cursor, end, weight))
        cursor = end

    total = sum(w for _, _, w in buckets)
    if total <= 0:
        # Profile covered no buckets in the window — fall back to uniform.
        n = len(buckets)
        return [(s, e, 1.0 / n) for s, e, _ in buckets] if n else []
    return [(s, e, w / total) for s, e, w in buckets]


def _floor_to_bucket(ts: datetime) -> datetime:
    minute = (ts.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    return ts.replace(minute=minute, second=0, microsecond=0)
