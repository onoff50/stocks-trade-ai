"""Slice plan generation and residual rebalancing."""
from __future__ import annotations

from datetime import datetime

from .types import Bucket, SlicePlan
from .volume_profile import build_schedule_weights


def build_slice_plan(
    total_qty: int,
    profile: dict,
    window_start: datetime,
    window_end: datetime,
) -> SlicePlan:
    """Convert (parent qty, volume profile, window) → per-bucket planned qty.

    Uses largest-remainder allocation so the integer slices sum exactly to total_qty.
    """
    if total_qty <= 0:
        raise ValueError("total_qty must be positive")

    weighted = build_schedule_weights(profile, window_start, window_end)
    if not weighted:
        raise ValueError("empty execution window")

    raw = [total_qty * w for _, _, w in weighted]
    floors = [int(x) for x in raw]
    remainder = total_qty - sum(floors)
    # Distribute leftover units to buckets with the largest fractional parts.
    order = sorted(
        range(len(raw)), key=lambda i: raw[i] - floors[i], reverse=True
    )
    for i in order[:remainder]:
        floors[i] += 1

    buckets = [
        Bucket(index=i, start=s, end=e, planned_qty=q)
        for i, ((s, e, _), q) in enumerate(zip(weighted, floors, strict=True))
    ]
    return SlicePlan(buckets=buckets, total_qty=total_qty)


def rebalance_residual(
    plan: SlicePlan,
    filled_through_index: int,
    actually_filled_qty: int,
) -> SlicePlan:
    """Re-spread under/over-fill across remaining buckets weighted by their planned qty.

    Returns a new SlicePlan with planned_qty updated for buckets after
    `filled_through_index`. Past buckets are left unchanged.
    """
    if filled_through_index < -1 or filled_through_index >= len(plan.buckets):
        raise ValueError("filled_through_index out of range")

    past = plan.buckets[: filled_through_index + 1]
    future = plan.buckets[filled_through_index + 1 :]
    target_remaining = plan.total_qty - actually_filled_qty
    if not future or target_remaining <= 0:
        # Either nothing left to schedule, or done early — zero out the future.
        future = [Bucket(b.index, b.start, b.end, 0) for b in future]
        return SlicePlan(buckets=past + future, total_qty=plan.total_qty)

    future_planned_total = sum(b.planned_qty for b in future)
    if future_planned_total <= 0:
        # No weight in the future — spread uniformly.
        n = len(future)
        base, rem = divmod(target_remaining, n)
        new_future = [
            Bucket(b.index, b.start, b.end, base + (1 if i < rem else 0))
            for i, b in enumerate(future)
        ]
        return SlicePlan(buckets=past + new_future, total_qty=plan.total_qty)

    raw = [target_remaining * (b.planned_qty / future_planned_total) for b in future]
    floors = [int(x) for x in raw]
    remainder = target_remaining - sum(floors)
    order = sorted(
        range(len(raw)), key=lambda i: raw[i] - floors[i], reverse=True
    )
    for i in order[:remainder]:
        floors[i] += 1
    new_future = [
        Bucket(b.index, b.start, b.end, q)
        for b, q in zip(future, floors, strict=True)
    ]
    return SlicePlan(buckets=past + new_future, total_qty=plan.total_qty)
