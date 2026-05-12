from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

IST = ZoneInfo("Asia/Kolkata")

DEFAULT_ENV_PATH = Path.home() / ".stocks-trade-ai" / ".env"
DEFAULT_STATE_DIR = Path.home() / ".stocks-trade-ai" / "state"


@dataclass(frozen=True, slots=True)
class Settings:
    groww_api_key: str
    groww_totp_secret: str
    state_dir: Path
    dashboard_bind: str
    log_level: str
    adv_cap_pct: float
    per_child_pct_of_5min_volume: float
    slippage_bps: float

    @property
    def dashboard_host(self) -> str:
        return self.dashboard_bind.split(":", 1)[0]

    @property
    def dashboard_port(self) -> int:
        return int(self.dashboard_bind.split(":", 1)[1])


def load_settings(env_path: Path | None = None) -> Settings:
    """Load settings from .env. Searches ~/.stocks-trade-ai/.env, then process env."""
    path = env_path or DEFAULT_ENV_PATH
    if path.exists():
        load_dotenv(path, override=False)

    api_key = os.environ.get("GROWW_API_KEY", "").strip()
    totp_secret = os.environ.get("GROWW_TOTP_SECRET", "").strip()
    if not api_key or not totp_secret:
        raise RuntimeError(
            f"Missing GROWW_API_KEY or GROWW_TOTP_SECRET. "
            f"Run `stocks-trade-ai setup` or edit {path}."
        )

    state_dir = Path(os.environ.get("STATE_DIR", str(DEFAULT_STATE_DIR))).expanduser()
    state_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        groww_api_key=api_key,
        groww_totp_secret=totp_secret,
        state_dir=state_dir,
        dashboard_bind=os.environ.get("DASHBOARD_BIND", "127.0.0.1:8080"),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        adv_cap_pct=float(os.environ.get("RISK_ADV_CAP_PCT", "10")),
        per_child_pct_of_5min_volume=float(
            os.environ.get("RISK_PER_CHILD_PCT_OF_5MIN_VOLUME", "1")
        ),
        slippage_bps=float(os.environ.get("RISK_SLIPPAGE_BPS", "30")),
    )


def write_env_file(api_key: str, totp_secret: str, path: Path | None = None) -> Path:
    """Write credentials to .env with chmod 600. Used by `stocks-trade-ai setup`."""
    target = path or DEFAULT_ENV_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        f"GROWW_API_KEY={api_key}\n"
        f"GROWW_TOTP_SECRET={totp_secret}\n"
    )
    target.chmod(0o600)
    return target
