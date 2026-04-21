"""Read-only data models over the PowerTrader file-based state.

Each model reads JSON/text files written by pt_trainer, pt_thinker, or pt_trader
and returns structured Python dicts.  Models cache by file mtime so repeated
reads within the same second are free.
"""

import json
import time
from pathlib import Path

from pt_env import PTEnv, TRAINING_STALENESS_DAYS


def _read_json(path: Path) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _read_jsonl(path: Path, limit: int = 250) -> list[dict]:
    try:
        with open(path) as f:
            lines = f.readlines()
        tail = lines[-limit:] if len(lines) > limit else lines
        out = []
        for line in tail:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return out
    except FileNotFoundError:
        return []


def _read_int(path: Path) -> int:
    try:
        return int(Path(path).read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def _read_float_list(path: Path) -> list[float]:
    try:
        text = Path(path).read_text().strip()
        if not text:
            return []
        return [float(x.strip()) for x in text.split(",") if x.strip()]
    except (FileNotFoundError, ValueError):
        return []


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


class _MtimeCache:
    """Cache a value keyed on file mtime."""

    def __init__(self):
        self._cache: dict[str, tuple[float, object]] = {}

    def get(self, path: Path, reader):
        key = str(path)
        mt = _mtime(path)
        cached = self._cache.get(key)
        if cached and cached[0] == mt:
            return cached[1]
        val = reader(path)
        self._cache[key] = (mt, val)
        return val


_cache = _MtimeCache()


class CoinModel:
    """Read-only view of a single coin's state."""

    def __init__(self, env: PTEnv, coin: str):
        self.env = env
        self.coin = coin

    def long_signal(self) -> int:
        return _cache.get(self.env.long_signal_path(self.coin), _read_int)

    def short_signal(self) -> int:
        return _cache.get(self.env.short_signal_path(self.coin), _read_int)

    def long_price_levels(self) -> list[float]:
        return _cache.get(self.env.low_bound_path(self.coin), _read_float_list)

    def short_price_levels(self) -> list[float]:
        return _cache.get(self.env.high_bound_path(self.coin), _read_float_list)

    def training_status(self) -> dict:
        data = _cache.get(self.env.trainer_status_path(self.coin), _read_json)
        return data or {"coin": self.coin, "state": "UNKNOWN"}

    def training_failure(self) -> dict | None:
        return _cache.get(self.env.trainer_failure_path(self.coin), _read_json)

    def is_trained(self) -> bool:
        path = self.env.trainer_time_path(self.coin)
        try:
            ts = float(path.read_text().strip())
            age_days = (time.time() - ts) / 86400
            return age_days <= TRAINING_STALENESS_DAYS
        except (FileNotFoundError, ValueError):
            return False

    def last_trained_ts(self) -> float:
        try:
            return float(self.env.trainer_time_path(self.coin).read_text().strip())
        except (FileNotFoundError, ValueError):
            return 0.0

    def snapshot(self) -> dict:
        ts = self.training_status()
        return {
            "coin": self.coin,
            "long_signal": self.long_signal(),
            "short_signal": self.short_signal(),
            "long_price_levels": self.long_price_levels(),
            "short_price_levels": self.short_price_levels(),
            "is_trained": self.is_trained(),
            "training_state": ts.get("state", "UNKNOWN"),
            "last_trained_ts": self.last_trained_ts(),
        }


class AccountModel:
    """Read-only view of trader status, positions, P&L, and trade history."""

    def __init__(self, env: PTEnv, exchange: str | None = None):
        self.env = env
        self.exchange = exchange or env.exchange

    def trader_status(self) -> dict | None:
        return _cache.get(self.env.trader_status_path(self.exchange), _read_json)

    def account_summary(self) -> dict:
        ts = self.trader_status()
        if not ts or "account" not in ts:
            return {}
        return ts["account"]

    def positions(self) -> dict:
        ts = self.trader_status()
        if not ts or "positions" not in ts:
            return {}
        return {k: v for k, v in ts["positions"].items() if v.get("quantity", 0) > 0}

    def all_positions(self) -> dict:
        ts = self.trader_status()
        if not ts or "positions" not in ts:
            return {}
        return ts["positions"]

    def pnl(self) -> dict:
        data = _cache.get(self.env.pnl_ledger_path(self.exchange), _read_json)
        return data or {}

    def trade_history(self, limit: int = 250) -> list[dict]:
        return _read_jsonl(self.env.trade_history_path(self.exchange), limit)

    def account_value_history(self, limit: int = 500) -> list[dict]:
        return _read_jsonl(self.env.account_history_path(self.exchange), limit)


class SystemModel:
    """Read-only view of system-level state: runner readiness, script health."""

    def __init__(self, env: PTEnv):
        self.env = env

    def runner_ready(self) -> dict:
        data = _cache.get(self.env.runner_ready_path(), _read_json)
        return data or {"ready": False, "stage": "unknown"}

    def neural_autorestart(self) -> dict:
        data = _cache.get(self.env.neural_autorestart_path(), _read_json)
        return data or {}

    def ema200(self) -> dict:
        data = _cache.get(self.env.ema200_path(), _read_json)
        return data or {}

    def settings(self) -> dict:
        return self.env.settings
