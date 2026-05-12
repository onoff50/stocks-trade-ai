"""SQLite-backed durable state for in-flight sessions; survives crash within the day."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiosqlite

from .config import IST
from .types import (
    Bucket,
    ChildOrder,
    Fill,
    OrderStatus,
    ParentOrder,
    Side,
    SlicePlan,
)

log = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS parent_order (
    session_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    segment TEXT NOT NULL,
    product TEXT NOT NULL,
    side TEXT NOT NULL,
    total_qty INTEGER NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    dry_run INTEGER NOT NULL,
    arrival_mid TEXT,
    pid INTEGER,
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS slice_bucket (
    session_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    planned_qty INTEGER NOT NULL,
    PRIMARY KEY (session_id, idx)
);

CREATE TABLE IF NOT EXISTS child_order (
    local_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    bucket_index INTEGER NOT NULL,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    price TEXT,
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    broker_order_id TEXT,
    placed_at TEXT,
    last_status_at TEXT,
    reject_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_child_session ON child_order(session_id);
CREATE INDEX IF NOT EXISTS idx_child_bucket ON child_order(session_id, bucket_index);

CREATE TABLE IF NOT EXISTS fill (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    child_local_id TEXT NOT NULL,
    qty INTEGER NOT NULL,
    price TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    trade_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_fill_child ON fill(child_local_id);

CREATE TABLE IF NOT EXISTS slippage_mark (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    realized_avg TEXT NOT NULL,
    arrival_mid TEXT NOT NULL,
    slippage_bps TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT
);
"""


def _iso(ts: datetime) -> str:
    return ts.isoformat()


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(IST)


def _dec(s: str | None) -> Decimal | None:
    return Decimal(s) if s is not None else None


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._conn:
            raise RuntimeError("StateStore is not open")
        return self._conn

    async def save_parent(self, parent: ParentOrder, pid: int, started_at: datetime) -> None:
        await self.conn.execute(
            """INSERT OR REPLACE INTO parent_order
               (session_id, symbol, exchange, segment, product, side, total_qty,
                window_start, window_end, dry_run, arrival_mid, pid, started_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                parent.session_id, parent.symbol, parent.exchange, parent.segment,
                parent.product, parent.side.value, parent.total_qty,
                _iso(parent.window_start), _iso(parent.window_end),
                1 if parent.dry_run else 0,
                str(parent.arrival_mid) if parent.arrival_mid else None,
                pid, _iso(started_at),
            ),
        )
        await self.conn.commit()

    async def save_slice_plan(self, session_id: str, plan: SlicePlan) -> None:
        await self.conn.execute("DELETE FROM slice_bucket WHERE session_id = ?", (session_id,))
        await self.conn.executemany(
            "INSERT INTO slice_bucket (session_id, idx, start_ts, end_ts, planned_qty) "
            "VALUES (?,?,?,?,?)",
            [
                (session_id, b.index, _iso(b.start), _iso(b.end), b.planned_qty)
                for b in plan.buckets
            ],
        )
        await self.conn.commit()

    async def upsert_child(self, session_id: str, child: ChildOrder) -> None:
        await self.conn.execute(
            """INSERT INTO child_order
               (local_id, session_id, bucket_index, side, qty, price, order_type,
                status, broker_order_id, placed_at, last_status_at, reject_reason)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(local_id) DO UPDATE SET
                 qty=excluded.qty,
                 price=excluded.price,
                 status=excluded.status,
                 broker_order_id=excluded.broker_order_id,
                 placed_at=excluded.placed_at,
                 last_status_at=excluded.last_status_at,
                 reject_reason=excluded.reject_reason""",
            (
                child.local_id, session_id, child.bucket_index, child.side.value,
                child.qty, str(child.price) if child.price is not None else None,
                child.order_type, child.status.value, child.broker_order_id,
                _iso(child.placed_at) if child.placed_at else None,
                _iso(child.last_status_at) if child.last_status_at else None,
                child.reject_reason,
            ),
        )
        await self.conn.commit()

    async def append_fill(self, child_local_id: str, fill: Fill) -> None:
        await self.conn.execute(
            "INSERT INTO fill (child_local_id, qty, price, timestamp, trade_id) "
            "VALUES (?,?,?,?,?)",
            (child_local_id, fill.qty, str(fill.price), _iso(fill.timestamp), fill.trade_id),
        )
        await self.conn.commit()

    async def append_slippage_mark(
        self, session_id: str, ts: datetime, realized_avg: Decimal,
        arrival_mid: Decimal, slippage_bps: Decimal,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO slippage_mark (session_id, timestamp, realized_avg, arrival_mid, slippage_bps) "
            "VALUES (?,?,?,?,?)",
            (session_id, _iso(ts), str(realized_avg), str(arrival_mid), str(slippage_bps)),
        )
        await self.conn.commit()

    async def log_event(self, session_id: str, ts: datetime, kind: str, payload: dict[str, Any] | None = None) -> None:
        await self.conn.execute(
            "INSERT INTO event_log (session_id, timestamp, kind, payload) VALUES (?,?,?,?)",
            (session_id, _iso(ts), kind, json.dumps(payload) if payload else None),
        )
        await self.conn.commit()

    async def mark_completed(self, session_id: str, ts: datetime) -> None:
        await self.conn.execute(
            "UPDATE parent_order SET completed_at = ? WHERE session_id = ?",
            (_iso(ts), session_id),
        )
        await self.conn.commit()

    async def load_parent(self, session_id: str) -> ParentOrder | None:
        async with self.conn.execute(
            "SELECT session_id, symbol, exchange, segment, product, side, total_qty,"
            " window_start, window_end, dry_run, arrival_mid"
            " FROM parent_order WHERE session_id = ?", (session_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return ParentOrder(
            session_id=row[0], symbol=row[1], exchange=row[2], segment=row[3],
            product=row[4], side=Side(row[5]), total_qty=row[6],
            window_start=_parse_ts(row[7]), window_end=_parse_ts(row[8]),
            dry_run=bool(row[9]), arrival_mid=_dec(row[10]),
        )

    async def load_slice_plan(self, session_id: str) -> SlicePlan | None:
        async with self.conn.execute(
            "SELECT idx, start_ts, end_ts, planned_qty FROM slice_bucket "
            "WHERE session_id = ? ORDER BY idx", (session_id,)
        ) as cur:
            rows = await cur.fetchall()
        if not rows:
            return None
        buckets = [
            Bucket(index=r[0], start=_parse_ts(r[1]), end=_parse_ts(r[2]), planned_qty=r[3])
            for r in rows
        ]
        parent = await self.load_parent(session_id)
        return SlicePlan(buckets=buckets, total_qty=parent.total_qty if parent else 0)

    async def load_children(self, session_id: str) -> list[ChildOrder]:
        async with self.conn.execute(
            "SELECT local_id, bucket_index, side, qty, price, order_type, status,"
            " broker_order_id, placed_at, last_status_at, reject_reason"
            " FROM child_order WHERE session_id = ? ORDER BY placed_at", (session_id,)
        ) as cur:
            rows = await cur.fetchall()
        children: list[ChildOrder] = []
        for r in rows:
            c = ChildOrder(
                local_id=r[0], bucket_index=r[1], side=Side(r[2]), qty=r[3],
                price=_dec(r[4]), order_type=r[5], status=OrderStatus(r[6]),
                broker_order_id=r[7],
                placed_at=_parse_ts(r[8]) if r[8] else None,
                last_status_at=_parse_ts(r[9]) if r[9] else None,
                reject_reason=r[10],
            )
            async with self.conn.execute(
                "SELECT qty, price, timestamp, trade_id FROM fill WHERE child_local_id = ?",
                (c.local_id,),
            ) as fcur:
                for fr in await fcur.fetchall():
                    c.fills.append(
                        Fill(qty=fr[0], price=Decimal(fr[1]), timestamp=_parse_ts(fr[2]), trade_id=fr[3])
                    )
            children.append(c)
        return children

    async def list_active_sessions(self, on_or_after: datetime) -> list[str]:
        async with self.conn.execute(
            "SELECT session_id FROM parent_order "
            "WHERE completed_at IS NULL AND started_at >= ?",
            (_iso(on_or_after),),
        ) as cur:
            rows = await cur.fetchall()
        return [r[0] for r in rows]
