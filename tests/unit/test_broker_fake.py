from decimal import Decimal

from stocks_trade_ai.broker import FakeBroker


async def test_fake_broker_place_and_cancel():
    fb = FakeBroker()
    r = await fb.place_limit_sell("X", "NSE", "CASH", "CNC", 10, Decimal("100"))
    assert r.broker_order_id.startswith("FAKE-")
    assert r.status == "OPEN"
    await fb.cancel(r.broker_order_id, "CASH")
    o = await fb.fetch_order(r.broker_order_id, "CASH")
    assert o["status"] == "CANCELLED"


async def test_fake_broker_partial_then_full_fill():
    fb = FakeBroker()
    r = await fb.place_limit_sell("X", "NSE", "CASH", "CNC", 10, Decimal("100"))
    fb.fill(r.broker_order_id, 6, Decimal("100"))
    o = await fb.fetch_order(r.broker_order_id, "CASH")
    assert o["status"] == "PARTIALLY_FILLED"
    fb.fill(r.broker_order_id, 4, Decimal("100"))
    o = await fb.fetch_order(r.broker_order_id, "CASH")
    assert o["status"] == "FILLED"


async def test_fake_broker_modify():
    fb = FakeBroker()
    r = await fb.place_limit_sell("X", "NSE", "CASH", "CNC", 10, Decimal("100"))
    await fb.modify_limit_price(r.broker_order_id, "CASH", 10, Decimal("99.50"))
    o = await fb.fetch_order(r.broker_order_id, "CASH")
    assert o["price"] == Decimal("99.50")
