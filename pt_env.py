"""Centralized environment and path resolution for PowerTrader_AI.

All file paths used for inter-process communication live here so that
pt_hub, pt_web, pt_models, and pt_controller share a single source of truth.
"""

import json
import os
from pathlib import Path


TIMEFRAMES = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]
DISPLAY_TIMEFRAMES = [
    "1min", "5min", "15min", "30min",
    "1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week",
]
TRAINING_STALENESS_DAYS = 14


class PTEnv:
    """Resolves all PowerTrader file paths from gui_settings.json."""

    def __init__(self, project_dir: str | Path | None = None,
                 settings_path: str | Path | None = None):
        if project_dir is None:
            project_dir = Path(__file__).resolve().parent
        self.project_dir = Path(project_dir).resolve()
        self.settings_path = (
            Path(settings_path) if settings_path
            else self.project_dir / "gui_settings.json"
        )
        self._reload_settings()

    def _reload_settings(self):
        if self.settings_path.exists():
            with open(self.settings_path) as f:
                self._settings = json.load(f)
        else:
            self._settings = {}

        raw_main = (self._settings.get("main_neural_dir") or "state").strip()
        if raw_main and not os.path.isabs(raw_main):
            self._main_dir = (self.project_dir / raw_main).resolve()
        else:
            self._main_dir = Path(raw_main).resolve() if raw_main else self.project_dir / "state"

        raw_hub = (self._settings.get("hub_data_dir") or "").strip()
        if raw_hub:
            if not os.path.isabs(raw_hub):
                self._hub_dir = (self.project_dir / raw_hub).resolve()
            else:
                self._hub_dir = Path(raw_hub).resolve()
        else:
            self._hub_dir = self._main_dir / "hub_data"

    def reload(self):
        self._reload_settings()

    @property
    def settings(self) -> dict:
        return dict(self._settings)

    @property
    def exchange(self) -> str:
        return (self._settings.get("exchange") or "demo").strip().lower()

    @property
    def coins(self) -> list[str]:
        return list(self._settings.get("coins") or [])

    @property
    def main_dir(self) -> Path:
        return self._main_dir

    @property
    def hub_data_dir(self) -> Path:
        return self._hub_dir

    @property
    def coins_dir(self) -> Path:
        return self._main_dir / "coins"

    def coin_dir(self, coin: str) -> Path:
        return self.coins_dir / coin

    # -- Per-coin signal/training files --

    def long_signal_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "long_dca_signal.txt"

    def short_signal_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "short_dca_signal.txt"

    def low_bound_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "low_bound_prices.html"

    def high_bound_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "high_bound_prices.html"

    def trainer_status_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "trainer_status.json"

    def trainer_failure_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "trainer_failure_info.json"

    def trainer_time_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "trainer_last_training_time.txt"

    def killer_path(self, coin: str) -> Path:
        return self.coin_dir(coin) / "killer.txt"

    # -- Hub-level state files --

    def runner_ready_path(self) -> Path:
        return self._hub_dir / "runner_ready.json"

    def neural_autorestart_path(self) -> Path:
        return self._hub_dir / "neural_autorestart_state.json"

    def ema200_path(self) -> Path:
        return self._hub_dir / "lth_daily_ema200.json"

    def trader_status_path(self, exchange: str | None = None) -> Path:
        xk = exchange or self.exchange
        return self._hub_dir / f"trader_status_{xk}.json"

    def trade_history_path(self, exchange: str | None = None) -> Path:
        xk = exchange or self.exchange
        return self._hub_dir / f"trade_history_{xk}.jsonl"

    def pnl_ledger_path(self, exchange: str | None = None) -> Path:
        xk = exchange or self.exchange
        return self._hub_dir / f"pnl_ledger_{xk}.json"

    def account_history_path(self, exchange: str | None = None) -> Path:
        xk = exchange or self.exchange
        return self._hub_dir / f"account_value_history_{xk}.jsonl"

    def bot_order_ids_path(self, exchange: str | None = None) -> Path:
        xk = exchange or self.exchange
        return self._hub_dir / f"bot_order_ids_{xk}.json"

    # -- Script paths --

    def script_path(self, key: str) -> Path:
        mapping = {
            "thinker": "script_neural_runner2",
            "trainer": "script_neural_trainer",
            "trader": "script_trader",
        }
        settings_key = mapping.get(key, key)
        filename = self._settings.get(settings_key, f"pt_{key}.py")
        p = Path(filename)
        if not p.is_absolute():
            p = self.project_dir / p
        return p
