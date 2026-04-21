"""
Demo exchange adapter.

Simulates trading using KuCoin market data prices with configurable slippage.
No real money is involved — all balances and fills are tracked in memory and
persisted to per-exchange data files by pt_trader.py.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple

from kucoin.client import Market

from exchange_api import ExchangeAdapter, OrderResult

_market = Market(url="https://api.kucoin.com")

# Default simulated account
_DEFAULT_STARTING_USD = 10000.0
_DEFAULT_SLIPPAGE_FACTOR = 0.001  # 0.1% per trade


class DemoAdapter(ExchangeAdapter):

    def __init__(self, starting_usd: float = _DEFAULT_STARTING_USD,
                 slippage_factor: float = _DEFAULT_SLIPPAGE_FACTOR,
                 state_path: Optional[str] = None):
        self._slippage = slippage_factor
        self._state_path = state_path

        self._usd_balance: float = starting_usd
        self._holdings: Dict[str, float] = {}
        self._orders: Dict[str, list] = {}

        if state_path:
            self._load_state()

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if not self._state_path or not os.path.isfile(self._state_path):
            return
        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._usd_balance = float(data.get("usd_balance", self._usd_balance))
            self._holdings = {k: float(v) for k, v in (data.get("holdings") or {}).items()}
            self._orders = data.get("orders", {})
        except Exception:
            pass

    def _save_state(self) -> None:
        if not self._state_path:
            return
        try:
            os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
            tmp = self._state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({
                    "usd_balance": self._usd_balance,
                    "holdings": self._holdings,
                    "orders": {k: v[-200:] for k, v in self._orders.items()},
                }, f, indent=2)
            os.replace(tmp, self._state_path)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Price from KuCoin
    # ------------------------------------------------------------------

    def _kucoin_price(self, base: str) -> Optional[float]:
        try:
            kucoin_sym = f"{base}-USDT"
            data = _market.get_kline(kucoin_sym, "1min")
            if data and len(data) > 0 and len(data[0]) >= 3:
                return float(data[0][2])  # close of most recent 1-min candle
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # ExchangeAdapter interface
    # ------------------------------------------------------------------

    def to_exchange_symbol(self, canonical: str) -> str:
        return canonical  # demo uses canonical format internally

    def to_canonical_symbol(self, exchange_sym: str) -> str:
        return exchange_sym

    def get_account_value(self) -> Optional[float]:
        total = self._usd_balance
        for base, qty in self._holdings.items():
            price = self._kucoin_price(base)
            if price and qty > 0:
                total += qty * price
        return total

    def get_buying_power(self) -> Optional[float]:
        return self._usd_balance

    def get_holdings(self) -> Dict[str, float]:
        return {k: v for k, v in self._holdings.items() if v > 1e-12}

    def get_price(self, symbols: List[str]) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
        buy_prices: Dict[str, float] = {}
        sell_prices: Dict[str, float] = {}
        valid: List[str] = []

        for canonical in symbols:
            base = self.base_from_canonical(canonical)
            if base == "USDC":
                continue
            price = self._kucoin_price(base)
            if price and price > 0:
                buy_prices[canonical] = price * (1.0 + self._slippage)
                sell_prices[canonical] = price * (1.0 - self._slippage)
                valid.append(canonical)

        return buy_prices, sell_prices, valid

    def place_buy(self, symbol: str, amount_usd: float) -> Optional[OrderResult]:
        base = self.base_from_canonical(symbol)
        price = self._kucoin_price(base)
        if not price or price <= 0:
            return None

        buy_price = price * (1.0 + self._slippage)
        qty = amount_usd / buy_price
        cost = qty * buy_price

        if cost > self._usd_balance:
            qty = self._usd_balance / buy_price
            cost = qty * buy_price

        if qty <= 0 or cost <= 0:
            return None

        self._usd_balance -= cost
        self._holdings[base] = self._holdings.get(base, 0.0) + qty

        order_id = str(uuid.uuid4())
        order_rec = {
            "id": order_id, "side": "buy", "symbol": symbol,
            "state": "filled", "qty": qty, "price": buy_price,
            "notional": round(cost, 2), "fees": 0.0,
            "ts": time.time(),
        }
        self._orders.setdefault(symbol, []).append(order_rec)
        self._save_state()

        return OrderResult(
            order_id=order_id, state="filled",
            filled_qty=qty, avg_price=buy_price,
            notional_usd=round(cost, 2), fees_usd=0.0,
            raw=order_rec,
        )

    def place_sell(self, symbol: str, qty: float) -> Optional[OrderResult]:
        base = self.base_from_canonical(symbol)
        held = self._holdings.get(base, 0.0)
        if qty > held:
            qty = held
        if qty <= 0:
            return None

        price = self._kucoin_price(base)
        if not price or price <= 0:
            return None

        sell_price = price * (1.0 - self._slippage)
        proceeds = qty * sell_price

        self._holdings[base] = held - qty
        if self._holdings[base] < 1e-12:
            self._holdings.pop(base, None)
        self._usd_balance += proceeds

        order_id = str(uuid.uuid4())
        order_rec = {
            "id": order_id, "side": "sell", "symbol": symbol,
            "state": "filled", "qty": qty, "price": sell_price,
            "notional": round(proceeds, 2), "fees": 0.0,
            "ts": time.time(),
        }
        self._orders.setdefault(symbol, []).append(order_rec)
        self._save_state()

        return OrderResult(
            order_id=order_id, state="filled",
            filled_qty=qty, avg_price=sell_price,
            notional_usd=round(proceeds, 2), fees_usd=0.0,
            raw=order_rec,
        )

    def get_orders(self, symbol: str) -> dict:
        return {"results": list(self._orders.get(symbol, []))}


def create_adapter() -> DemoAdapter:
    hub_data = os.environ.get(
        "POWERTRADER_HUB_DIR",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "state", "hub_data"),
    )
    state_path = os.path.join(hub_data, "demo_exchange_state.json")

    settings_path = os.environ.get("POWERTRADER_GUI_SETTINGS") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "gui_settings.json"
    )

    starting_usd = _DEFAULT_STARTING_USD
    slippage = _DEFAULT_SLIPPAGE_FACTOR

    try:
        if os.path.isfile(settings_path):
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            starting_usd = float(data.get("demo_starting_usd", starting_usd))
            slippage = float(data.get("demo_slippage_factor", slippage))
    except Exception:
        pass

    return DemoAdapter(
        starting_usd=starting_usd,
        slippage_factor=slippage,
        state_path=state_path,
    )
