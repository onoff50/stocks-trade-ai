"""Standalone live-quote monitor with WebSocket push + 5-level depth.

The producer reads the active `GrowwMarketData.latest_payload` every POLL_SEC,
parses it into a snapshot, and broadcasts to all WS subscribers via a hub. The
hub also keeps a ring buffer (~5 minutes) for backfill on reconnect.

The same FastAPI app also exposes sessions endpoints so dry-run sell sessions
can be created from the UI; sessions run in-process via SessionRegistry.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import time as _time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, field_validator, model_validator

from ..config import IST
from ..market_data import (
    GrowwMarketData,
    MarketData,
    _extract_depth_payload,
    _sorted_levels,
)
from .server import summarize_session
from .session_registry import SellRequest, SessionRegistry

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

POLL_SEC = 0.5
RING_SIZE = 600          # ~5 min at 0.5s
DEPTH_LEVELS = 5
PER_CLIENT_QUEUE = 256   # drop oldest if a client is slow


def _build_snapshot(payload_or_raw: Any) -> dict[str, Any] | None:
    """Parse a depth payload (or a raw nested feed dict) into a wire snapshot."""
    if isinstance(payload_or_raw, dict) and (
        "buyBook" in payload_or_raw or "sellBook" in payload_or_raw
        or "buy" in payload_or_raw or "sell" in payload_or_raw
    ):
        payload = payload_or_raw
    else:
        payload = _extract_depth_payload(payload_or_raw)
    if payload is None:
        return None
    bids = _sorted_levels(
        payload.get("buyBook") or payload.get("buy") or payload.get("bids")
    )[:DEPTH_LEVELS]
    asks = _sorted_levels(
        payload.get("sellBook") or payload.get("sell") or payload.get("asks")
    )[:DEPTH_LEVELS]
    if not bids or not asks:
        return None
    mid = (bids[0]["price"] + asks[0]["price"]) / 2.0
    spread_abs = asks[0]["price"] - bids[0]["price"]
    spread_bps = (spread_abs / mid * 10_000.0) if mid > 0 else 0.0
    ltp = payload.get("ltp")
    feed_ts_ms = payload.get("tsInMillis")
    return {
        "ts": datetime.now(tz=IST).isoformat(),
        "feed_ts_ms": float(feed_ts_ms) if feed_ts_ms is not None else None,
        "bids": bids,
        "asks": asks,
        "mid": mid,
        "spread_abs": spread_abs,
        "spread_bps": spread_bps,
        "last_trade": float(ltp) if ltp is not None else None,
    }


class DepthHub:
    """In-memory pub/sub + ring buffer for depth + control frames.

    Each tick gets a monotonic seq. WS clients can reconnect and ask for
    `since_seq` to get backfill from the ring before live streaming resumes.
    Identical consecutive snapshots are skipped to avoid stuffing the ring with
    duplicates from the poll loop. `broadcast()` sends an arbitrary control
    frame (e.g. `symbol_changed`) without touching the seq counter or ring.
    """

    def __init__(self, ring_size: int = RING_SIZE) -> None:
        self._history: deque[dict[str, Any]] = deque(maxlen=ring_size)
        self._seq = 0
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._last_key: tuple[float, float, float | None] | None = None

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def last(self) -> dict[str, Any] | None:
        if not self._history:
            return None
        return self._history[-1]["snapshot"]

    def reset(self) -> None:
        self._history.clear()
        self._last_key = None
        self._seq = 0

    def add(self, snap: dict[str, Any]) -> bool:
        """Add snapshot if it differs from the last one; broadcast to subs."""
        key = (
            snap["bids"][0]["price"], snap["asks"][0]["price"],
            snap.get("last_trade"),
        )
        if key == self._last_key:
            return False
        self._seq += 1
        self._last_key = key
        snap = dict(snap)
        snap["seq"] = self._seq
        frame = {"type": "tick", "snapshot": snap}
        self._history.append(frame)
        self._enqueue_all(frame)
        return True

    def broadcast(self, frame: dict[str, Any]) -> None:
        """Send a control frame (not stored in ring, no seq)."""
        self._enqueue_all(frame)

    def _enqueue_all(self, frame: dict[str, Any]) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(frame)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(frame)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=PER_CLIENT_QUEUE)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(q)

    def history_after(self, last_seq: int) -> list[dict[str, Any]]:
        return [f for f in self._history if f["snapshot"]["seq"] > last_seq]


@dataclass
class MonitorState:
    """Mutable holder so the WS handler and producer see the current symbol."""
    market_data: GrowwMarketData
    hub: DepthHub

    @property
    def symbol(self) -> str:
        return self.market_data.trading_symbol


async def producer_loop(state: MonitorState) -> None:
    """Poll the SDK's cached depth and push new snapshots into the hub."""
    while True:
        try:
            payload = state.market_data.latest_payload
            if payload is None:
                payload = state.market_data.latest_raw_depth
            snap = _build_snapshot(payload)
            if snap is not None:
                state.hub.add(snap)
        except Exception as exc:
            log.warning("monitor producer error: %s", exc)
        await asyncio.sleep(POLL_SEC)


SESSION_COOKIE = "stai_session"
SESSION_TTL_SEC = 7 * 24 * 3600  # 7 days


class _NeedsLoginRedirect(Exception):
    """Signals an HTML page request needs to be redirected to /login."""
    def __init__(self, next_path: str) -> None:
        super().__init__("login required")
        self.next_path = next_path or "/"


def _is_auth_enabled() -> bool:
    return bool(os.environ.get("MONITOR_USER") and os.environ.get("MONITOR_PASS"))


def _signing_secret() -> bytes:
    """Server-side HMAC key derived from MONITOR_PASS so it survives restarts
    without an extra env var. We hash the password rather than use it raw so the
    plaintext isn't reused as a signing key."""
    pw = os.environ.get("MONITOR_PASS", "")
    return hashlib.sha256(("stai-monitor-cookie:" + pw).encode()).digest()


def _mint_session(username: str, ttl_sec: int = SESSION_TTL_SEC) -> str:
    """Returns a self-contained signed token: `username.exp.hmac_hex`."""
    exp = int(_time.time()) + ttl_sec
    payload = f"{username}.{exp}"
    sig = hmac.new(_signing_secret(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def _verify_session(token: str | None) -> bool:
    """Returns True iff `token` is a valid, non-expired session for MONITOR_USER."""
    if not _is_auth_enabled():
        return True
    if not token:
        return False
    parts = token.split(".")
    if len(parts) != 3:
        return False
    username, exp_str, sig_hex = parts
    expected_user = os.environ.get("MONITOR_USER", "")
    if not secrets.compare_digest(username, expected_user):
        return False
    try:
        exp = int(exp_str)
    except ValueError:
        return False
    if exp < int(_time.time()):
        return False
    expected_sig = hmac.new(
        _signing_secret(), f"{username}.{exp}".encode(), hashlib.sha256,
    ).hexdigest()
    return secrets.compare_digest(sig_hex, expected_sig)


def _check_login(username: str, password: str) -> bool:
    expected_user = os.environ.get("MONITOR_USER", "")
    expected_pass = os.environ.get("MONITOR_PASS", "")
    if not expected_user or not expected_pass:
        return False
    return (
        secrets.compare_digest(username, expected_user)
        and secrets.compare_digest(password, expected_pass)
    )


LOGIN_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>StockTrade · sign in</title>
<style>
  :root { color-scheme: dark; }
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
         background: #0d1117; color: #e6edf3; margin: 0;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; font-size: 13px; }
  .login-card { background: #161b22; border: 1px solid #30363d; padding: 32px;
                border-radius: 8px; width: 320px; }
  .brand { display: flex; align-items: center; gap: 10px; margin-bottom: 24px; }
  .brand-mark { width: 32px; height: 24px; color: #58a6ff; }
  .brand-name { font-size: 16px; font-weight: 700; letter-spacing: 0.3px; }
  .brand-sub { color: #7d8590; font-size: 11px; margin-left: auto; }
  .row { display: flex; flex-direction: column; gap: 4px; margin-bottom: 14px; }
  label { color: #7d8590; font-size: 11px; }
  input { background: #0d1117; color: #e6edf3; border: 1px solid #30363d;
          border-radius: 4px; padding: 6px 8px; font: inherit; font-size: 13px;
          box-sizing: border-box; width: 100%; height: 30px; }
  input:focus { outline: none; border-color: #58a6ff; }
  button { background: #1f6f3b; color: #fff; border: 0; padding: 0 18px;
           border-radius: 4px; cursor: pointer; height: 30px; font: inherit;
           font-size: 13px; font-weight: 500; width: 100%; }
  button:hover { background: #2ea043; }
  .err { color: #f85149; font-size: 11px; margin-bottom: 12px; min-height: 14px; }
</style></head><body>
<form class="login-card" method="POST" action="/login">
  <div class="brand">
    <svg class="brand-mark" viewBox="0 0 32 24" aria-hidden="true">
      <line x1="5" y1="2" x2="5" y2="22" stroke="currentColor" stroke-width="1" opacity="0.4"/>
      <line x1="13" y1="2" x2="13" y2="22" stroke="currentColor" stroke-width="1" opacity="0.4"/>
      <line x1="21" y1="2" x2="21" y2="22" stroke="currentColor" stroke-width="1" opacity="0.4"/>
      <line x1="29" y1="2" x2="29" y2="22" stroke="currentColor" stroke-width="1" opacity="0.4"/>
      <rect x="2"  y="4"  width="6" height="14" fill="#f85149" rx="1"/>
      <rect x="10" y="8"  width="6" height="11" fill="#f85149" opacity="0.7" rx="1"/>
      <rect x="18" y="12" width="6" height="8"  fill="#f85149" opacity="0.55" rx="1"/>
      <rect x="26" y="6"  width="6" height="16" fill="#2ea043" rx="1"/>
    </svg>
    <span class="brand-name">StockTrade</span>
    <span class="brand-sub">sign in</span>
  </div>
  <div class="err">__ERROR__</div>
  <div class="row">
    <label for="username">Username</label>
    <input id="username" name="username" autocomplete="username" required autofocus>
  </div>
  <div class="row">
    <label for="password">Password</label>
    <input id="password" name="password" type="password"
           autocomplete="current-password" required>
  </div>
  <input type="hidden" name="next" value="__NEXT__">
  <button type="submit">Sign in</button>
</form>
</body></html>"""


def _render_login(error: str = "", next_path: str = "/") -> HTMLResponse:
    # next_path is user-controlled — only allow same-origin relative paths so
    # the form can't be weaponised into an open-redirect.
    if not next_path.startswith("/") or next_path.startswith("//"):
        next_path = "/"
    html = (
        LOGIN_PAGE
        .replace("__ERROR__", _html_escape(error))
        .replace("__NEXT__", _html_escape(next_path))
    )
    # 200 OK (not 401) — avoids any Chrome-side heuristics that flag bare 401s.
    return HTMLResponse(html, status_code=200)


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace('"', "&quot;").replace("'", "&#39;")
    )


def _is_https_request(request: Request) -> bool:
    return (
        request.url.scheme == "https"
        or request.headers.get("x-forwarded-proto") == "https"
    )


class _SymbolBody(BaseModel):
    symbol: str = Field(min_length=1, max_length=32)

    @field_validator("symbol")
    @classmethod
    def _upper_strip(cls, v: str) -> str:
        return v.strip().upper()


class _SellBody(BaseModel):
    symbol: str = Field(min_length=1, max_length=32)
    qty: int = Field(gt=0, le=10_000_000)
    until: str = Field(min_length=4, max_length=5, description="HH:MM IST")
    start: str | None = Field(default=None, description="HH:MM IST, defaults to now")
    product: str = Field(default="CNC")
    allow_no_adv_cap: bool = True
    child_min_qty: int | None = Field(default=None, gt=0, le=10_000_000)
    child_max_qty: int | None = Field(default=None, gt=0, le=10_000_000)
    # Optional price floor — engine skips child placement while bid < this.
    min_price: float | None = Field(default=None, gt=0, le=10_000_000)
    # dry_run defaults to True. Live trades require dry_run=False AND the
    # ALLOW_LIVE_TRADES env var set; the route handler enforces the env gate.
    dry_run: bool = True

    @field_validator("symbol")
    @classmethod
    def _upper_strip(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("product")
    @classmethod
    def _product_allowed(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in {"CNC", "MIS"}:
            raise ValueError("product must be CNC or MIS")
        return v

    @model_validator(mode="after")
    def _child_bounds_consistent(self) -> "_SellBody":
        # Both set or both unset; max >= min when set.
        if (self.child_min_qty is None) != (self.child_max_qty is None):
            raise ValueError(
                "child_min_qty and child_max_qty must both be set or both blank",
            )
        if (
            self.child_min_qty is not None
            and self.child_max_qty is not None
            and self.child_max_qty < self.child_min_qty
        ):
            raise ValueError("child_max_qty must be >= child_min_qty")
        return self


def _parse_hhmm_today(hhmm: str) -> datetime:
    h_str, _, m_str = hhmm.partition(":")
    h, m = int(h_str), int(m_str)
    return datetime.combine(datetime.now(tz=IST).date(), time(h, m), tzinfo=IST)


_SECURITY_HEADERS = {
    # Force HTTPS for the next 2 years; submit to browser preload list eligibility.
    # Only emitted on HTTPS requests so http://localhost dev still works.
    "Strict-Transport-Security": "max-age=63072000; includeSubDomains",
    # Prevent MIME-sniffing.
    "X-Content-Type-Options": "nosniff",
    # Block embedding in iframes (defends against UI redress / clickjacking).
    "X-Frame-Options": "DENY",
    # Trim what we leak in the Referer header to other origins.
    "Referrer-Policy": "strict-origin-when-cross-origin",
    # Restrict powerful browser APIs we don't use.
    "Permissions-Policy": (
        "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
        "magnetometer=(), microphone=(), payment=(), usb=()"
    ),
    # Belt-and-suspenders CSP. We use a small amount of inline CSS/JS in the
    # template (no external scripts) so 'unsafe-inline' is required for those.
    # connect-src must allow wss to the same origin so the WS handshake passes.
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self' wss: ws:; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'"
    ),
}


def create_monitor_app(
    state: MonitorState, registry: SessionRegistry | None,
) -> FastAPI:
    app = FastAPI(title=f"StockTrade · {state.symbol}")

    @app.middleware("http")
    async def _security_headers(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        # Pull HSTS off if the inbound request is plain HTTP (don't poison
        # a non-TLS connection with an HSTS commitment).
        is_https = (
            request.url.scheme == "https"
            or request.headers.get("x-forwarded-proto") == "https"
        )
        for header, value in _SECURITY_HEADERS.items():
            if header == "Strict-Transport-Security" and not is_https:
                continue
            response.headers.setdefault(header, value)
        return response
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    async def require_auth(request: Request) -> None:
        if not _is_auth_enabled():
            return
        token = request.cookies.get(SESSION_COOKIE)
        if not _verify_session(token):
            # API endpoints should get JSON 401; HTML pages get a redirect to
            # /login. Distinguish by the Accept header.
            accept = request.headers.get("accept", "")
            if "text/html" in accept:
                # Use a special exception that the handler below turns into a
                # 303 redirect to /login?next=<original>.
                raise _NeedsLoginRedirect(str(request.url.path))
            raise HTTPException(status_code=401, detail="Authentication required")

    @app.exception_handler(_NeedsLoginRedirect)
    async def _redirect_to_login(request: Request, exc: _NeedsLoginRedirect) -> Any:
        return RedirectResponse(
            url=f"/login?next={exc.next_path}", status_code=303,
        )

    @app.get("/login", response_class=HTMLResponse)
    async def login_get(request: Request, next: str = "/") -> Any:
        # If already signed in, jump straight to the destination.
        if _verify_session(request.cookies.get(SESSION_COOKIE)):
            return RedirectResponse(url=next or "/", status_code=303)
        return _render_login(next_path=next or "/")

    @app.post("/login", response_class=HTMLResponse)
    async def login_post(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        next: str = Form("/"),
    ) -> Any:
        if not _is_auth_enabled():
            return RedirectResponse(url=next or "/", status_code=303)
        if not _check_login(username, password):
            return _render_login(error="Invalid username or password", next_path=next or "/")
        target = next if next.startswith("/") and not next.startswith("//") else "/"
        resp = RedirectResponse(url=target, status_code=303)
        resp.set_cookie(
            key=SESSION_COOKIE,
            value=_mint_session(username),
            max_age=SESSION_TTL_SEC,
            httponly=True,
            secure=_is_https_request(request),
            samesite="strict",
            path="/",
        )
        return resp

    @app.post("/logout")
    async def logout(request: Request) -> Any:
        resp = RedirectResponse(url="/login", status_code=303)
        resp.delete_cookie(SESSION_COOKIE, path="/")
        return resp

    @app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_auth)])
    async def index(request: Request) -> Any:
        md = state.market_data
        return templates.TemplateResponse(
            request, "monitor.html",
            {
                "symbol": md.trading_symbol,
                "exchange": md._exchange, "segment": md._segment,
                "depth_levels": DEPTH_LEVELS,
                "auth_enabled": _is_auth_enabled(),
                "sessions_enabled": registry is not None,
            },
        )

    @app.get("/quote", response_class=JSONResponse, dependencies=[Depends(require_auth)])
    async def quote_http() -> Any:
        return {
            "ok": state.hub.last is not None,
            "snapshot": state.hub.last,
            "symbol": state.symbol,
            "now": datetime.now(tz=IST).isoformat(),
        }

    @app.post("/api/symbol", dependencies=[Depends(require_auth)])
    async def switch_symbol(body: _SymbolBody) -> Any:
        try:
            instrument = await state.market_data.switch_symbol(body.symbol)
        except KeyError:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown symbol", "symbol": body.symbol},
            )
        # Hub history is now stale (different instrument). Reset, then notify
        # any open page so it can clear its local cache.
        state.hub.reset()
        state.hub.broadcast({
            "type": "symbol_changed",
            "symbol": state.market_data.trading_symbol,
            "exchange": state.market_data._exchange,
            "segment": state.market_data._segment,
        })
        return {
            "ok": True,
            "symbol": state.market_data.trading_symbol,
            "exchange_token": instrument.get("exchange_token"),
        }

    @app.get("/api/sessions", dependencies=[Depends(require_auth)])
    async def list_sessions() -> Any:
        if registry is None:
            return {"sessions": []}
        out = []
        for sess in registry.list_sessions():
            try:
                data = await summarize_session(sess.state_store, sess.session_id)
            except Exception as exc:
                log.warning("summarize failed for %s: %s", sess.session_id, exc)
                data = None
            running = not sess.task.done()
            if data is None:
                # Fall back to in-memory parent if state DB hasn't been written yet.
                p = sess.parent
                data = {
                    "session_id": sess.session_id,
                    "symbol": p.symbol,
                    "total_qty": p.total_qty,
                    "filled_qty": 0,
                    "filled_pct": 0.0,
                    "arrival_mid": str(p.arrival_mid) if p.arrival_mid else None,
                    "avg_fill_price": None,
                    "slippage_bps": None,
                    "dry_run": p.dry_run,
                    "window_start": p.window_start.isoformat(),
                    "window_end": p.window_end.isoformat(),
                    "buckets": [], "children": [],
                    "now": datetime.now(tz=IST).isoformat(),
                }
            data["status"] = (
                "error" if sess.error else
                "running" if running else
                "completed"
            )
            data["error"] = sess.error
            data["started_at"] = sess.started_at.isoformat()
            data["ended_at"] = sess.ended_at.isoformat() if sess.ended_at else None
            out.append(data)
        return {"sessions": out}

    @app.get("/api/sessions/{session_id}", dependencies=[Depends(require_auth)])
    async def get_session(session_id: str) -> Any:
        if registry is None:
            raise HTTPException(status_code=404, detail="sessions disabled")
        sess = registry.get(session_id)
        if sess is None:
            raise HTTPException(status_code=404, detail="session not found")
        data = await summarize_session(sess.state_store, session_id)
        if data is None:
            raise HTTPException(status_code=404, detail="state not initialised")
        data["status"] = (
            "error" if sess.error else
            "running" if not sess.task.done() else
            "completed"
        )
        data["error"] = sess.error
        return data

    @app.post("/api/sessions", dependencies=[Depends(require_auth)])
    async def create_session(body: _SellBody) -> Any:
        if registry is None:
            raise HTTPException(status_code=503, detail="sessions disabled")
        # Server-side live-trade gate: even if a client sends dry_run=False,
        # the server only honors it when the operator explicitly enabled live
        # trading by starting the process with ALLOW_LIVE_TRADES=1. This
        # protects against a fat-fingered client.
        if not body.dry_run and os.environ.get("ALLOW_LIVE_TRADES") != "1":
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "live trading disabled on this server",
                    "hint": "restart with ALLOW_LIVE_TRADES=1 to enable",
                },
            )
        try:
            start_dt = _parse_hhmm_today(body.start) if body.start else datetime.now(tz=IST)
            end_dt = _parse_hhmm_today(body.until)
            if end_dt <= start_dt:
                raise HTTPException(
                    status_code=422,
                    detail={"error": "window_end must be after window_start"},
                )
            from decimal import Decimal as _D
            req = SellRequest(
                symbol=body.symbol, qty=body.qty,
                window_start=start_dt, window_end=end_dt,
                product=body.product,
                allow_no_adv_cap=body.allow_no_adv_cap,
                child_min_qty=body.child_min_qty,
                child_max_qty=body.child_max_qty,
                min_price=_D(str(body.min_price)) if body.min_price is not None else None,
                dry_run=body.dry_run,
            )
            if not body.dry_run:
                log.warning(
                    "*** LIVE SESSION REQUESTED *** symbol=%s qty=%d window=%s..%s",
                    body.symbol, body.qty,
                    start_dt.isoformat(timespec="seconds"),
                    end_dt.isoformat(timespec="seconds"),
                )
            sess = await registry.start(req)
        except KeyError as exc:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown symbol", "symbol": str(exc.args[0])},
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"error": str(exc)})
        return {"session_id": sess.session_id, "dry_run": body.dry_run}

    @app.post("/api/sessions/{session_id}/kill", dependencies=[Depends(require_auth)])
    async def kill_session(session_id: str) -> Any:
        if registry is None:
            raise HTTPException(status_code=404, detail="sessions disabled")
        ok = await registry.kill(session_id)
        if not ok:
            raise HTTPException(status_code=404, detail="session not found")
        return {"ok": True}

    @app.websocket("/ws/depth")
    async def ws_depth(ws: WebSocket) -> None:
        # Browsers attach cookies to WS upgrade requests, so the same session
        # cookie that authenticates the page also authenticates the WebSocket.
        if not _verify_session(ws.cookies.get(SESSION_COOKIE)):
            await ws.close(code=1008)  # policy violation
            return
        await ws.accept()
        q = state.hub.subscribe()
        send_task: asyncio.Task[None] | None = None
        try:
            since = 0
            try:
                hello = await asyncio.wait_for(ws.receive_json(), timeout=2.0)
                since = int(hello.get("since_seq", 0))
            except (TimeoutError, ValueError, KeyError):
                since = 0
            except WebSocketDisconnect:
                return
            backfill = state.hub.history_after(since)
            await ws.send_json({
                "type": "hello",
                "server_seq": state.hub.seq,
                "backfill_count": len(backfill),
                "symbol": state.market_data.trading_symbol,
                "exchange": state.market_data._exchange,
                "segment": state.market_data._segment,
            })
            for frame in backfill:
                await ws.send_json(frame)

            async def _live_send() -> None:
                while True:
                    frame = await q.get()
                    await ws.send_json(frame)

            async def _recv_watchdog() -> None:
                while True:
                    await ws.receive_text()

            send_task = asyncio.create_task(_live_send())
            recv_task = asyncio.create_task(_recv_watchdog())
            done, pending = await asyncio.wait(
                {send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            log.warning("ws error: %s", exc)
        finally:
            state.hub.unsubscribe(q)
            if send_task and not send_task.done():
                send_task.cancel()

    return app
