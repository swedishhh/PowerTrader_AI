"""
Control/Demo exchange adapter.

Simulates zero-friction trading as a baseline: zero fees, zero bid/ask spread,
trades execute at mid-price using the configured live price source.

Used in both modes:
  - Demo mode   : this IS the user's account, labelled "Demo"
  - Trading mode: runs alongside real exchanges as a frictionless comparison baseline

Balances and fills are tracked in memory and persisted to a state file.
All configuration (price_source, starting_usd, state_path) is passed in by the
caller (pt_web.py) — this module has no dependency on any specific exchange.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple

from exchange_api import ExchangeAdapter, OrderResult
from price_source import get_mid_price

_DEFAULT_STARTING_USD = 10_000.0


class ControlAdapter(ExchangeAdapter):

    def __init__(
        self,
        starting_usd: float = _DEFAULT_STARTING_USD,
        price_source: str = "kucoin",
        state_path: Optional[str] = None,
    ):
        self._price_source = price_source
        self._state_path = state_path

        self._usd_balance: float = starting_usd
        self._holdings: Dict[str, float] = {}
        self._orders: Dict[str, list] = {}

        if state_path:
            self._load_state()

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
                    "orders": dict(self._orders),
                }, f, indent=2)
            os.replace(tmp, self._state_path)
        except Exception:
            pass

    def _get_price(self, base: str) -> Optional[float]:
        return get_mid_price(base, self._price_source)

    def to_exchange_symbol(self, canonical: str) -> str:
        return canonical

    def to_canonical_symbol(self, exchange_sym: str) -> str:
        return exchange_sym

    def get_account_value(self) -> Optional[float]:
        total = self._usd_balance
        for base, qty in self._holdings.items():
            price = self._get_price(base)
            if price and qty > 0:
                total += qty * price
        return total

    def get_buying_power(self) -> Optional[float]:
        return self._usd_balance

    def get_holdings(self) -> Dict[str, float]:
        self._load_state()
        return {k: v for k, v in self._holdings.items() if v > 1e-12}

    def get_price(self, symbols: List[str]) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
        buy_prices: Dict[str, float] = {}
        sell_prices: Dict[str, float] = {}
        valid: List[str] = []

        for canonical in symbols:
            base = self.base_from_canonical(canonical)
            if base in ("USD", "USDT"):
                continue
            price = self._get_price(base)
            if price and price > 0:
                buy_prices[canonical] = price
                sell_prices[canonical] = price
                valid.append(canonical)

        return buy_prices, sell_prices, valid

    def place_buy(self, symbol: str, amount_usd: float) -> Optional[OrderResult]:
        base = self.base_from_canonical(symbol)
        price = self._get_price(base)
        if not price or price <= 0:
            return None

        qty = amount_usd / price
        cost = qty * price

        if cost > self._usd_balance:
            qty = self._usd_balance / price
            cost = qty * price

        if qty <= 0 or cost <= 0:
            return None

        self._usd_balance -= cost
        self._holdings[base] = self._holdings.get(base, 0.0) + qty

        order_id = str(uuid.uuid4())
        order_rec = {
            "id": order_id, "side": "buy", "symbol": symbol,
            "state": "filled", "qty": qty, "price": price,
            "notional": cost, "fees": 0.0,
            "ts": time.time(),
        }
        self._orders.setdefault(symbol, []).append(order_rec)
        self._save_state()

        return OrderResult(
            order_id=order_id, state="filled",
            filled_qty=qty, avg_price=price,
            notional_usd=cost, fees_usd=0.0,
            raw=order_rec,
        )

    def place_sell(self, symbol: str, qty: float) -> Optional[OrderResult]:
        base = self.base_from_canonical(symbol)
        held = self._holdings.get(base, 0.0)
        if qty > held:
            qty = held
        if qty <= 0:
            return None

        price = self._get_price(base)
        if not price or price <= 0:
            return None

        proceeds = qty * price

        self._holdings[base] = held - qty
        if self._holdings[base] < 1e-12:
            self._holdings.pop(base, None)
        self._usd_balance += proceeds

        order_id = str(uuid.uuid4())
        order_rec = {
            "id": order_id, "side": "sell", "symbol": symbol,
            "state": "filled", "qty": qty, "price": price,
            "notional": proceeds, "fees": 0.0,
            "ts": time.time(),
        }
        self._orders.setdefault(symbol, []).append(order_rec)
        self._save_state()

        return OrderResult(
            order_id=order_id, state="filled",
            filled_qty=qty, avg_price=price,
            notional_usd=proceeds, fees_usd=0.0,
            raw=order_rec,
        )

    def get_orders(self, symbol: str) -> dict:
        return {"results": list(self._orders.get(symbol, []))}


def create_adapter(
    starting_usd: float = _DEFAULT_STARTING_USD,
    price_source: str = "kucoin",
    state_path: Optional[str] = None,
) -> ControlAdapter:
    """Factory used by pt_web.py. All config is passed in — no side-effects here."""
    return ControlAdapter(
        starting_usd=starting_usd,
        price_source=price_source,
        state_path=state_path,
    )
