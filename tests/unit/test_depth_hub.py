import asyncio

import pytest

from stocks_trade_ai.dashboard.monitor import DepthHub, _build_snapshot


def _snap(bid: float, ask: float) -> dict:
    """Minimal snapshot shape that DepthHub.add() expects."""
    return {
        "ts": "x", "feed_ts_ms": None,
        "bids": [{"price": bid, "qty": 1}],
        "asks": [{"price": ask, "qty": 1}],
        "mid": (bid + ask) / 2,
        "spread_abs": ask - bid, "spread_bps": 0.0,
        "last_trade": None,
    }


def test_hub_dedups_identical_consecutive():
    hub = DepthHub(ring_size=10)
    assert hub.add(_snap(100.0, 100.1)) is True
    assert hub.add(_snap(100.0, 100.1)) is False
    assert hub.seq == 1


def test_hub_advances_seq_on_change():
    hub = DepthHub(ring_size=10)
    hub.add(_snap(100.0, 100.1))
    hub.add(_snap(100.0, 100.2))
    assert hub.seq == 2


def test_history_after_returns_only_newer_frames():
    hub = DepthHub(ring_size=10)
    for i in range(5):
        hub.add(_snap(100.0 + i * 0.01, 100.1 + i * 0.01))
    after = hub.history_after(2)
    assert [f["snapshot"]["seq"] for f in after] == [3, 4, 5]


def test_reset_clears_history_and_seq():
    hub = DepthHub(ring_size=10)
    hub.add(_snap(100.0, 100.1))
    hub.add(_snap(100.0, 100.2))
    hub.reset()
    assert hub.seq == 0
    assert hub.last is None
    assert hub.history_after(0) == []


async def test_subscribers_receive_tick_and_control_frames():
    hub = DepthHub(ring_size=10)
    q = hub.subscribe()
    hub.add(_snap(100.0, 100.1))
    hub.broadcast({"type": "symbol_changed", "symbol": "RELIANCE"})
    f1 = await asyncio.wait_for(q.get(), timeout=0.5)
    f2 = await asyncio.wait_for(q.get(), timeout=0.5)
    assert f1["type"] == "tick"
    assert f1["snapshot"]["bids"][0]["price"] == 100.0
    assert f2["type"] == "symbol_changed"
    assert f2["symbol"] == "RELIANCE"


def test_build_snapshot_real_groww_shape():
    # The real Groww depth shape: nested {exchange: {segment: {token: payload}}}
    # with buyBook/sellBook as dicts keyed by "1".."5" level strings.
    raw = {
        "NSE": {"CASH": {"759806": {
            "tsInMillis": 1778577633521,
            "buyBook": {
                "1": {"price": 182.95, "qty": 26},
                "2": {"price": 182.94, "qty": 313},
                "3": {"price": 182.91, "qty": 97},
            },
            "sellBook": {
                "1": {"price": 182.97, "qty": 2436},
                "2": {"price": 182.98, "qty": 2857},
            },
        }}},
    }
    snap = _build_snapshot(raw)
    assert snap is not None
    assert snap["bids"][0] == {"price": 182.95, "qty": 26}
    assert snap["asks"][0] == {"price": 182.97, "qty": 2436}
    assert snap["mid"] == pytest.approx(182.96)
    assert snap["spread_abs"] == pytest.approx(0.02, abs=1e-6)


def test_build_snapshot_payload_direct():
    """The producer can also pass the already-extracted payload directly."""
    payload = {
        "buyBook": {"1": {"price": 100.0, "qty": 5}},
        "sellBook": {"1": {"price": 100.5, "qty": 7}},
        "ltp": 100.25,
    }
    snap = _build_snapshot(payload)
    assert snap is not None
    assert snap["bids"][0]["price"] == 100.0
    assert snap["asks"][0]["price"] == 100.5
    assert snap["last_trade"] == 100.25
