"""
PaperExchange — frictionless simulated exchange.

Simulates zero-friction trading as a baseline: zero fees, zero bid/ask spread,
trades execute at mid-price using the configured live price source.

Used in two roles:
  - Demo mode   : PaperExchange IS the user's account (key='demo')
  - Trading mode: PaperExchange runs as the shadow account inside ShadowedExchange (key='shadow'),
                  providing a frictionless comparison baseline against the real exchange

ShadowedExchange — wraps a real Exchange + a PaperExchange shadow.

Every fill on the real exchange is automatically mirrored into the shadow at
mid-price with zero fees. The delta between real and shadow quantifies
real-world friction (spread + fees). All trades are mirrored — no exceptions.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from exchange_api import Exchange, OrderResult
from price_source import get_mid_price

_DEFAULT_STARTING_USD = 10_000.0


class PaperExchange(Exchange):

    def __init__(
        self,
        key: str = "demo",
        starting_usd: float = _DEFAULT_STARTING_USD,
        price_source: str = "kucoin",
        state_path: Optional[str] = None,
    ):
        self._key = key
        self._price_source = price_source
        self._state_path = state_path

        self._usd_balance: float = starting_usd
        self._holdings: Dict[str, float] = {}
        self._orders: Dict[str, list] = {}
        self._position_cost: Dict[str, dict] = {}  # coin -> {"usd_cost": float, "qty": float}

        if state_path:
            self._load_state()

    @property
    def key(self) -> str:
        return self._key

    @property
    def display_name(self) -> str:
        return "Demo" if self._key == "demo" else "Shadow"

    @property
    def is_paper(self) -> bool:
        return True

    def _load_state(self) -> None:
        if not self._state_path or not os.path.isfile(self._state_path):
            return
        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._usd_balance = float(data.get("usd_balance", self._usd_balance))
            self._holdings = {k: float(v) for k, v in (data.get("holdings") or {}).items()}
            self._orders = data.get("orders", {})
            self._position_cost = data.get("position_cost", {})
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
                    "position_cost": self._position_cost,
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

        pos = self._position_cost.setdefault(base, {"usd_cost": 0.0, "qty": 0.0})
        pos["usd_cost"] = float(pos.get("usd_cost", 0)) + cost
        pos["qty"] = float(pos.get("qty", 0)) + qty

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

        pos = self._position_cost.get(base, {})
        old_qty = float(pos.get("qty", 0))
        if old_qty > 0:
            frac = min(1.0, qty / old_qty)
            pos["usd_cost"] = float(pos.get("usd_cost", 0)) * (1 - frac)
            pos["qty"] = old_qty - qty
            if pos["qty"] <= 1e-12:
                self._position_cost.pop(base, None)

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

    # ------------------------------------------------------------------
    # Status / history helpers (called by ShadowedExchange.tick())
    # ------------------------------------------------------------------

    def _state_dir(self) -> Optional[Path]:
        if not self._state_path:
            return None
        return Path(self._state_path).parent

    def _append_account_value(self, total: float) -> None:
        d = self._state_dir()
        if not d:
            return
        try:
            with open(d / "account_value_history.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": time.time(), "total_account_value": total}) + "\n")
        except Exception:
            pass

    def write_status(self) -> None:
        """Write trader_status.json from current paper state + live prices."""
        d = self._state_dir()
        if not d:
            return

        ts = time.time()
        total = self.get_account_value() or 0

        positions = {}
        for base, qty in self._holdings.items():
            if qty <= 1e-12:
                continue
            price = self._get_price(base)
            if not price or price <= 0:
                continue
            pos_data = self._position_cost.get(base, {})
            pos_cost = float(pos_data.get("usd_cost", 0))
            pos_qty = float(pos_data.get("qty", 0))
            avg_cost = pos_cost / pos_qty if pos_qty > 0 else 0.0
            value = qty * price
            pnl_pct = ((price - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0.0
            positions[base] = {
                "quantity": qty,
                "avg_cost_basis": avg_cost,
                "current_buy_price": price,
                "current_sell_price": price,
                "value_usd": value,
                "gain_loss_pct_buy": pnl_pct,
                "gain_loss_pct_sell": pnl_pct,
                "dca_triggered_stages": 0,
                "trail_active": False,
                "trail_line": 0.0,
            }

        status = {
            "timestamp": ts,
            "account": {
                "total_account_value": total,
                "buying_power": self._usd_balance,
                "holdings_sell_value": total - self._usd_balance,
                "holdings_buy_value": total - self._usd_balance,
                "percent_in_trade": (
                    (1 - self._usd_balance / total) * 100 if total > 0 else 0
                ),
                "pm_start_pct_no_dca": 0.0,
                "pm_start_pct_with_dca": 0.0,
                "trailing_gap_pct": 0.0,
            },
            "positions": positions,
        }

        tmp = d / "trader_status.json.tmp"
        try:
            tmp.write_text(json.dumps(status, indent=2))
            tmp.replace(d / "trader_status.json")
        except Exception:
            pass


class ShadowedExchange(Exchange):
    """Wraps a real Exchange; every fill is mirrored into a frictionless PaperExchange shadow.

    The delta between real and shadow quantifies friction (spread + fees).
    All trades are mirrored — no exceptions.
    """

    def __init__(self, real: Exchange, shadow: PaperExchange):
        self._real = real
        self._shadow = shadow
        self._last_tick_ts: float = 0.0

    @property
    def key(self) -> str:
        return self._real.key

    @property
    def display_name(self) -> str:
        return self._real.display_name

    @property
    def is_paper(self) -> bool:
        return False

    def all_accounts(self) -> List[Exchange]:
        return [self, self._shadow]

    def tick(self) -> None:
        """Called each manage_trades() loop; runs shadow maintenance every 5 minutes."""
        now = time.time()
        if now - self._last_tick_ts < 300:
            return
        self._last_tick_ts = now
        try:
            total = self._shadow.get_account_value() or 0
            if total > 0:
                self._shadow._append_account_value(total)
            self._shadow.write_status()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Mirrored trade operations
    # ------------------------------------------------------------------

    def place_buy(self, symbol: str, amount_usd: float) -> Optional[OrderResult]:
        result = self._real.place_buy(symbol, amount_usd)
        if result:
            try:
                notional = (
                    result.notional_usd
                    or ((result.avg_price or 0) * (result.filled_qty or 0))
                    or amount_usd
                )
                self._shadow.place_buy(symbol, notional)
            except Exception:
                pass
        return result

    def place_sell(self, symbol: str, qty: float) -> Optional[OrderResult]:
        result = self._real.place_sell(symbol, qty)
        if result:
            base = self._real.base_from_canonical(symbol)
            shadow_qty = self._shadow.get_holdings().get(base, 0)
            if shadow_qty > 1e-12:
                try:
                    self._shadow.place_sell(symbol, shadow_qty)
                except Exception:
                    pass
        return result

    # ------------------------------------------------------------------
    # Delegate everything else to the real exchange
    # ------------------------------------------------------------------

    def get_account_value(self) -> Optional[float]:
        return self._real.get_account_value()

    def get_buying_power(self) -> Optional[float]:
        return self._real.get_buying_power()

    def get_holdings(self) -> Dict[str, float]:
        return self._real.get_holdings()

    def get_price(self, symbols: List[str]) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
        return self._real.get_price(symbols)

    def get_orders(self, symbol: str) -> dict:
        return self._real.get_orders(symbol)

    def get_order_result(self, symbol: str, order_id: str) -> Optional[OrderResult]:
        return self._real.get_order_result(symbol, order_id)

    def has_valid_trading_pairs(self) -> bool:
        return self._real.has_valid_trading_pairs()

    def calculate_cost_basis_from_orders(self, *args, **kwargs) -> Dict[str, float]:
        return self._real.calculate_cost_basis_from_orders(*args, **kwargs)

    def get_filled_bot_buy_qty(self, *args, **kwargs) -> float:
        return self._real.get_filled_bot_buy_qty(*args, **kwargs)

    def get_min_order_cost(self, symbol: str) -> float:
        return self._real.get_min_order_cost(symbol)

    def to_exchange_symbol(self, canonical: str) -> str:
        return self._real.to_exchange_symbol(canonical)

    def to_canonical_symbol(self, exchange_sym: str) -> str:
        return self._real.to_canonical_symbol(exchange_sym)


def create_paper_exchange(
    key: str = "demo",
    starting_usd: float = _DEFAULT_STARTING_USD,
    price_source: str = "kucoin",
    state_path: Optional[str] = None,
) -> PaperExchange:
    """Factory used by pt_web.py. All config is passed in — no side-effects here."""
    return PaperExchange(
        key=key,
        starting_usd=starting_usd,
        price_source=price_source,
        state_path=state_path,
    )
