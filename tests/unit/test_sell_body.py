"""Pydantic validation tests for the /api/sessions request body."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from stocks_trade_ai.dashboard.monitor import _SellBody


def _base() -> dict:
    return {
        "symbol": "RELIANCE",
        "qty": 100,
        "until": "15:25",
        "product": "CNC",
    }


def test_sell_body_accepts_no_child_bounds():
    body = _SellBody(**_base())
    assert body.child_min_qty is None
    assert body.child_max_qty is None


def test_sell_body_accepts_consistent_child_bounds():
    body = _SellBody(**_base(), child_min_qty=5, child_max_qty=15)
    assert body.child_min_qty == 5
    assert body.child_max_qty == 15


def test_sell_body_rejects_only_min_set():
    with pytest.raises(ValidationError, match="both be set or both blank"):
        _SellBody(**_base(), child_min_qty=5)


def test_sell_body_rejects_only_max_set():
    with pytest.raises(ValidationError, match="both be set or both blank"):
        _SellBody(**_base(), child_max_qty=15)


def test_sell_body_rejects_max_below_min():
    with pytest.raises(ValidationError, match=">= child_min_qty"):
        _SellBody(**_base(), child_min_qty=10, child_max_qty=3)


def test_sell_body_allows_min_equals_max():
    body = _SellBody(**_base(), child_min_qty=7, child_max_qty=7)
    assert body.child_min_qty == body.child_max_qty == 7


def test_sell_body_rejects_negative_or_zero_min():
    with pytest.raises(ValidationError):
        _SellBody(**_base(), child_min_qty=0, child_max_qty=10)
    with pytest.raises(ValidationError):
        _SellBody(**_base(), child_min_qty=-1, child_max_qty=10)


# ---------- dry_run / live mode ---------------------------------------------


def test_sell_body_dry_run_defaults_to_true():
    body = _SellBody(**_base())
    assert body.dry_run is True


def test_sell_body_accepts_explicit_dry_run():
    body = _SellBody(**_base(), dry_run=True)
    assert body.dry_run is True


def test_sell_body_accepts_live_flag():
    # Schema-level acceptance — the live env-gate is enforced in the route,
    # not the schema. Pydantic just normalizes the boolean.
    body = _SellBody(**_base(), dry_run=False)
    assert body.dry_run is False
