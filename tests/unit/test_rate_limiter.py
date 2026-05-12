
import pytest

from stocks_trade_ai.rate_limiter import RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


async def _take_n(rl: RateLimiter, n: int) -> None:
    for _ in range(n):
        await rl.acquire()


async def test_within_caps_no_wait(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr("stocks_trade_ai.rate_limiter.asyncio.sleep", fake_sleep)
    clock = FakeClock()
    rl = RateLimiter(per_sec=5, per_min=100, clock=clock)
    await _take_n(rl, 5)
    assert sleeps == []


async def test_blocks_when_per_sec_exceeded(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock.advance(s)  # simulate time advancing

    clock = FakeClock()
    monkeypatch.setattr("stocks_trade_ai.rate_limiter.asyncio.sleep", fake_sleep)
    rl = RateLimiter(per_sec=2, per_min=100, clock=clock)
    # 3 in burst — 3rd should sleep ~1s
    for _ in range(3):
        await rl.acquire()
    assert sleeps and sleeps[0] == pytest.approx(1.0, abs=1e-6)


async def test_per_minute_cap_dominates(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock.advance(s)

    clock = FakeClock()
    monkeypatch.setattr("stocks_trade_ai.rate_limiter.asyncio.sleep", fake_sleep)
    rl = RateLimiter(per_sec=100, per_min=3, clock=clock)
    for _ in range(3):
        await rl.acquire()
        clock.advance(0.1)
    # 4th call must wait for the minute window to roll.
    await rl.acquire()
    assert sleeps and sleeps[0] > 50  # ~60s minus elapsed


async def test_negative_args_rejected():
    with pytest.raises(ValueError):
        RateLimiter(per_sec=0, per_min=1)
