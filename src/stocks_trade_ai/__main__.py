"""CLI entry point: setup, sell, status, kill."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import typer

from . import __version__
from .auth import build_client
from .config import DEFAULT_ENV_PATH, IST, load_settings, write_env_file
from .engine import Engine, install_signal_handlers, new_session_id
from .state_store import StateStore
from .types import ParentOrder, Side

app = typer.Typer(add_completion=False, no_args_is_help=True, help=f"stocks-trade-ai v{__version__}")


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _redact_logger() -> None:
    """Filter log records to strip anything that looks like a token/secret/key."""
    needle = ("token", "secret", "key", "authorization")

    class _F(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            msg = str(record.getMessage()).lower()
            return not any(n in msg and len(msg) > 24 for n in needle)

    logging.getLogger().addFilter(_F())


@app.command()
def setup(
    env_path: Path = typer.Option(DEFAULT_ENV_PATH, help="Path to write .env file."),  # noqa: B008
) -> None:
    """Interactively store Groww API key + TOTP secret."""
    typer.secho(f"Writing credentials to {env_path}", fg="cyan")
    api_key = typer.prompt("GROWW_API_KEY", hide_input=True)
    totp_secret = typer.prompt("GROWW_TOTP_SECRET", hide_input=True)
    target = write_env_file(api_key.strip(), totp_secret.strip(), env_path)
    typer.secho(f"Wrote {target} (chmod 600)", fg="green")


@app.command()
def sell(
    symbol: str = typer.Option(..., help="Trading symbol, e.g. RELIANCE"),
    qty: int = typer.Option(..., help="Total quantity to sell (parent order)"),
    until: str = typer.Option(..., help="Window end, HH:MM IST (e.g. 15:00)"),
    start: str | None = typer.Option(None, help="Window start, HH:MM IST. Default: now."),
    exchange: str = typer.Option("NSE", help="NSE or BSE"),
    segment: str = typer.Option("CASH"),
    product: str = typer.Option("CNC", help="CNC for delivery, MIS for intraday"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Log intended orders without placing"),
) -> None:
    """Sell `qty` of `symbol` using VWAP between start and until."""
    settings = load_settings()
    _setup_logging(settings.log_level)
    _redact_logger()

    now = datetime.now(tz=IST)
    start_dt = _parse_hhmm_today(start) if start else now
    end_dt = _parse_hhmm_today(until)
    if end_dt <= start_dt:
        typer.secho("--until must be after --start (or after now)", fg="red")
        raise typer.Exit(2)

    parent = ParentOrder(
        session_id=new_session_id(), symbol=symbol.upper(), exchange=exchange,
        segment=segment, product=product, side=Side.SELL, total_qty=qty,
        window_start=start_dt, window_end=end_dt, dry_run=dry_run, arrival_mid=None,
    )
    # GrowwFeed.__init__ runs its own asyncio loop synchronously; must build the
    # client BEFORE entering asyncio.run, or it raises "loop already running".
    api, feed = build_client(settings)
    asyncio.run(_run_sell(settings, parent, api, feed))


async def _run_sell(settings, parent: ParentOrder, api, feed) -> None:
    from .broker import GrowwBroker
    from .market_data import GrowwMarketData
    from .rate_limiter import RateLimiter
    from .volume_profile import median_volume_profile

    limiter = RateLimiter(per_sec=8, per_min=200, name="orders")
    broker = GrowwBroker(api, limiter)
    md = GrowwMarketData(
        api, feed, exchange=parent.exchange, trading_symbol=parent.symbol, segment=parent.segment,
    )
    await md.start()

    # Pull arrival mid from the first quote available (up to 10 seconds).
    for _ in range(20):
        q = await md.latest_quote()
        if q:
            parent = _replace_arrival_mid(parent, q.mid)
            break
        await asyncio.sleep(0.5)

    # Volume profile + ADV from the last 20 trading days of 5-min candles.
    bars, adv = await _fetch_history(api, parent)
    profile = median_volume_profile(bars)

    state = StateStore(settings.state_dir / f"{parent.session_id}.db")
    await state.open()
    dashboard_task = asyncio.create_task(_start_dashboard(settings, parent.session_id))
    try:
        engine = Engine(
            settings=settings, broker=broker, market_data=md, state=state, parent=parent,
            adv_20day=adv, volume_profile=profile,
        )
        install_signal_handlers(engine)
        await engine.run()
    finally:
        dashboard_task.cancel()
        try:
            await dashboard_task
        except (asyncio.CancelledError, Exception):
            pass
        await md.aclose()
        await state.close()


async def _start_dashboard(settings, session_id: str) -> None:
    import uvicorn

    from .dashboard.server import create_app

    cfg = uvicorn.Config(
        create_app(settings, session_id),
        host=settings.dashboard_host, port=settings.dashboard_port,
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(cfg)
    typer.secho(f"Dashboard: http://{settings.dashboard_bind}/", fg="cyan")
    await server.serve()


def _replace_arrival_mid(p: ParentOrder, mid):
    return ParentOrder(
        session_id=p.session_id, symbol=p.symbol, exchange=p.exchange, segment=p.segment,
        product=p.product, side=p.side, total_qty=p.total_qty,
        window_start=p.window_start, window_end=p.window_end, dry_run=p.dry_run,
        arrival_mid=mid,
    )


async def _fetch_history(api, parent: ParentOrder):
    """Fetch ~20 days of 5-min candles. Returns ([], 0.0) and warns if the API
    key lacks the historical-data scope (caller should set allow_no_adv_cap)."""
    from datetime import timedelta

    from growwapi.groww.exceptions import GrowwAPIException

    from .types import OHLCBar

    end = parent.window_start
    start = end - timedelta(days=30)  # 30 calendar to cover 20 trading days
    # SDK requires the namespaced groww_symbol (e.g. "NSE-GROWW"), not the bare ticker.
    instrument = await asyncio.to_thread(
        api.get_instrument_by_exchange_and_trading_symbol,
        parent.exchange, parent.symbol,
    )
    groww_symbol = instrument["groww_symbol"] if instrument else parent.symbol
    # SDK wants 'yyyy-MM-dd HH:mm:ss' (no T, no tz offset).
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        resp = await asyncio.to_thread(
            api.get_historical_candles,
            exchange=parent.exchange, segment=parent.segment,
            groww_symbol=groww_symbol,
            start_time=start.strftime(fmt), end_time=end.strftime(fmt),
            candle_interval=api.CANDLE_INTERVAL_MIN_5,
        )
    except GrowwAPIException as exc:
        msg = str(exc).lower()
        if "forbidden" in msg or "permission" in msg or "unauthorized" in msg:
            logging.getLogger(__name__).warning(
                "Historical-data scope missing for this API key (%s). "
                "Engine will run with uniform 5-min profile and no ADV cap. "
                "Upgrade the Groww API tier to enable ADV-based risk checks.",
                exc,
            )
            return [], 0.0
        raise
    raw = resp.get("candles") or resp.get("data") or []
    bars: list[OHLCBar] = []
    daily: dict[date, int] = {}
    for row in raw:
        if isinstance(row, dict):
            ts_raw = row["timestamp"]
            o_raw, h_raw, lo_raw, c_raw = (
                row["open"], row["high"], row["low"], row["close"],
            )
            v_raw = row["volume"]
        else:  # list-of-lists: [ts, o, h, l, c, v, ...]
            ts_raw, o_raw, h_raw, lo_raw, c_raw, v_raw = row[0:6]
        # Skip rows with no actual trades (Groww returns pre/post-session bars
        # with None OHLC but a stub volume — these aren't real candles).
        if o_raw is None or h_raw is None or lo_raw is None or c_raw is None:
            continue
        # Timestamp is either an ISO-8601 string ("2026-04-23T09:00:00") or
        # an epoch integer; accept both.
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(int(ts_raw), tz=IST)
        else:
            ts = datetime.fromisoformat(str(ts_raw))
            ts = ts.astimezone(IST) if ts.tzinfo else ts.replace(tzinfo=IST)
        o = Decimal(str(o_raw))
        h = Decimal(str(h_raw))
        lo = Decimal(str(lo_raw))
        c = Decimal(str(c_raw))
        v = int(v_raw or 0)
        bars.append(OHLCBar(start=ts, open=o, high=h, low=lo, close=c, volume=v))
        daily[ts.date()] = daily.get(ts.date(), 0) + v
    adv = (sum(daily.values()) / len(daily)) if daily else 0.0
    return bars, adv


def _parse_hhmm_today(hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    t = time(int(h), int(m))
    return datetime.combine(datetime.now(tz=IST).date(), t, tzinfo=IST)


@app.command()
def monitor(
    symbol: str = typer.Option(..., help="Trading symbol, e.g. RELIANCE"),
    exchange: str = typer.Option("NSE", help="NSE or BSE"),
    segment: str = typer.Option("CASH"),
    bind: str = typer.Option("127.0.0.1:8080", help="host:port for dashboard"),
) -> None:
    """Live market-data monitor for a single symbol (no trading, no historical data)."""
    settings = load_settings()
    _setup_logging(settings.log_level)
    _redact_logger()
    # build_client must run before asyncio.run (GrowwFeed inits its own loop).
    api, feed = build_client(settings)
    asyncio.run(_run_monitor(settings, api, feed, symbol.upper(), exchange, segment, bind))


async def _run_monitor(settings, api, feed, symbol, exchange, segment, bind) -> None:
    import uvicorn

    from .dashboard.monitor import DepthHub, MonitorState, create_monitor_app, producer_loop
    from .dashboard.session_registry import SessionRegistry
    from .market_data import GrowwMarketData

    md = GrowwMarketData(
        api, feed, exchange=exchange, trading_symbol=symbol, segment=segment,
    )
    await md.start()
    hub = DepthHub()
    state = MonitorState(market_data=md, hub=hub)
    registry = SessionRegistry(settings, api, feed, history_fetcher=_fetch_history)
    producer_task = asyncio.create_task(producer_loop(state), name="monitor-producer")

    host, port = bind.split(":", 1)
    cfg = uvicorn.Config(
        create_monitor_app(state, registry),
        host=host, port=int(port),
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(cfg)
    typer.secho(f"StockTrade: http://{bind}/  ({symbol} on {exchange})", fg="cyan")
    try:
        await server.serve()
    finally:
        producer_task.cancel()
        try:
            await producer_task
        except (asyncio.CancelledError, Exception):
            pass
        await registry.shutdown()
        await md.aclose()


@app.command()
def status() -> None:
    """Show currently-active sessions and their progress."""
    settings = load_settings()
    asyncio.run(_show_status(settings))


async def _show_status(settings) -> None:
    today_start = datetime.combine(datetime.now(tz=IST).date(), time.min, tzinfo=IST)
    # Walk all session DB files in state_dir.
    for db in sorted(settings.state_dir.glob("vwap-*.db")):
        s = StateStore(db)
        await s.open()
        try:
            for sid in await s.list_active_sessions(today_start):
                parent = await s.load_parent(sid)
                children = await s.load_children(sid)
                filled = sum(c.filled_qty for c in children)
                pct = (filled / parent.total_qty * 100) if parent else 0
                typer.echo(
                    f"{sid}  {parent.symbol if parent else '?'}  "
                    f"{filled}/{parent.total_qty if parent else '?'} ({pct:.1f}%)"
                )
        finally:
            await s.close()


@app.command()
def kill(session_id: str = typer.Argument(..., help="Session ID to terminate")) -> None:
    """Send SIGTERM to a running session by ID."""
    settings = load_settings()
    asyncio.run(_kill(settings, session_id))


async def _kill(settings, session_id: str) -> None:
    db = settings.state_dir / f"{session_id}.db"
    if not db.exists():
        typer.secho(f"session not found: {db}", fg="red")
        raise typer.Exit(2)
    s = StateStore(db)
    await s.open()
    try:
        async with s.conn.execute(
            "SELECT pid FROM parent_order WHERE session_id = ?", (session_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row or not row[0]:
            typer.secho("no PID recorded for this session", fg="red")
            raise typer.Exit(2)
        pid = int(row[0])
        try:
            os.kill(pid, signal.SIGTERM)
            typer.secho(f"sent SIGTERM to pid {pid}", fg="yellow")
        except ProcessLookupError:
            typer.secho(f"process {pid} not running", fg="red")
    finally:
        await s.close()


if __name__ == "__main__":
    sys.exit(app())
