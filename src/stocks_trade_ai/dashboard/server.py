"""Local FastAPI dashboard for monitoring a running session."""
from __future__ import annotations

import logging
import os
import signal
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..config import IST, Settings
from ..state_store import StateStore

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


async def summarize_session(store: StateStore, session_id: str) -> dict[str, Any] | None:
    """Render the durable state of one session for JSON consumption.

    Returns None when the session has no parent record yet (caller can 404).
    """
    parent = await store.load_parent(session_id)
    if not parent:
        return None
    plan = await store.load_slice_plan(session_id)
    children = await store.load_children(session_id)
    filled = sum(c.filled_qty for c in children)
    notional = sum((f.price * f.qty for c in children for f in c.fills), start=0)
    avg = (float(notional) / filled) if filled else None
    arrival_mid_f = float(parent.arrival_mid) if parent.arrival_mid else None
    slippage_bps = None
    if avg is not None and arrival_mid_f:
        slippage_bps = (arrival_mid_f - avg) / arrival_mid_f * 10_000.0
    return {
        "session_id": session_id,
        "symbol": parent.symbol,
        "exchange": parent.exchange,
        "segment": parent.segment,
        "product": parent.product,
        "side": parent.side.value,
        "total_qty": parent.total_qty,
        "filled_qty": filled,
        "filled_pct": filled / parent.total_qty * 100 if parent.total_qty else 0,
        "arrival_mid": str(parent.arrival_mid) if parent.arrival_mid else None,
        "avg_fill_price": avg,
        "slippage_bps": slippage_bps,
        "dry_run": parent.dry_run,
        "window_start": parent.window_start.isoformat(),
        "window_end": parent.window_end.isoformat(),
        "buckets": [
            {
                "index": b.index, "planned": b.planned_qty,
                "start": b.start.isoformat(), "end": b.end.isoformat(),
                "filled": sum(
                    c.filled_qty for c in children if c.bucket_index == b.index
                ),
            }
            for b in (plan.buckets if plan else [])
        ],
        "children": [
            {
                "id": c.local_id, "bucket": c.bucket_index,
                "qty": c.qty, "filled": c.filled_qty,
                "price": str(c.price) if c.price else None,
                "status": c.status.value, "broker_id": c.broker_order_id,
            }
            for c in children
        ],
        "now": datetime.now(tz=IST).isoformat(),
    }


def create_app(settings: Settings, session_id: str) -> FastAPI:
    app = FastAPI(title=f"stocks-trade-ai  {session_id}")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    db_path = settings.state_dir / f"{session_id}.db"

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> Any:
        return templates.TemplateResponse(request, "index.html", {"session_id": session_id})

    @app.get("/state", response_class=JSONResponse)
    async def state() -> Any:
        store = StateStore(db_path)
        await store.open()
        try:
            data = await summarize_session(store, session_id)
            if data is None:
                raise HTTPException(status_code=404, detail="session not found")
            return data
        finally:
            await store.close()

    @app.post("/kill")
    async def kill() -> Any:
        store = StateStore(db_path)
        await store.open()
        try:
            async with store.conn.execute(
                "SELECT pid FROM parent_order WHERE session_id = ?", (session_id,),
            ) as cur:
                row = await cur.fetchone()
            if not row or not row[0]:
                raise HTTPException(status_code=404, detail="pid not recorded")
            pid = int(row[0])
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError as e:
                raise HTTPException(status_code=410, detail="process gone") from e
            return {"sent_signal": "SIGTERM", "pid": pid}
        finally:
            await store.close()

    return app
