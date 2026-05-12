"""FastAPI web server for PowerTrader_AI.

Run:

    python pt_web.py [--port 8080]

    conda activate powertrader && cd ~/dev/code/git/PowerTrader_AI && python pt_web.py --port 8088

Provides REST API + WebSocket for real-time updates, serving the web/ frontend.

Modes
-----
Demo    : only the control (frictionless) adapter runs; shown as "Demo" in the UI.
Trading : control adapter runs alongside the real exchanges in env.exchanges.
"""

import asyncio
import json
import time
import argparse
import pandas as pd
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from pt_env import PTEnv
from pt_models import CoinModel, AccountModel, SystemModel
from pt_controller import ProcessController
from control_mirror import ControlMirror

PROJECT_DIR = Path(__file__).resolve().parent
WEB_DIR = PROJECT_DIR / "web"

env = PTEnv(project_dir=PROJECT_DIR)
ctrl = ProcessController(env)
app = FastAPI(title="PowerTrader Web")


def _get_mirror() -> ControlMirror:
    return ControlMirror(str(env.hub_data_dir))

try:
    _get_mirror().write_status()
except Exception:
    pass

_adapters: dict = {}


def _get_adapter(xk: str):
    if xk not in _adapters:
        try:
            if xk in ("control", "demo"):
                # Both use ControlAdapter; state files are namespaced by key (demo vs control)
                from exchange_control import create_adapter as _make_ctrl
                state_path = str(env.exchange_state_path(xk))
                cfg = env.get_config()
                price_source = cfg.get("live_price_source", "kucoin")
                starting_usd = float(cfg.get("control_starting_usd") or 0)
                _adapters[xk] = _make_ctrl(
                    starting_usd=starting_usd,
                    price_source=price_source,
                    state_path=state_path,
                )
            else:
                import importlib
                mod = importlib.import_module(f"exchange_{xk}")
                _adapters[xk] = mod.create_adapter()
        except Exception as e:
            print(f"[Adapter] failed to create {xk}: {e}")
            return None
    return _adapters.get(xk)


def _is_demo_mode() -> bool:
    return env.trading_mode == "demo"


def _active_exchanges() -> list[str]:
    """All exchanges active for the current mode.

    Demo mode  : ["demo"]   — state files under hub_data/demo/
    Trading mode: ["control"] + real exchanges — state files under hub_data/{xk}/
    """
    if _is_demo_mode():
        return ["demo"]
    return ["control"] + env.exchanges


def _ctrl_xk() -> str:
    """Key for the synthetic frictionless adapter in the current mode: 'demo' or 'control'."""
    return "demo" if _is_demo_mode() else "control"


def _control_sync_exchange() -> str:
    """The real exchange to sync the control starting balance from."""
    cfg = env.get_config()
    xk = (cfg.get("control_sync_exchange") or "").strip().lower()
    if not xk:
        real = env.exchanges
        xk = real[0] if real else ""
    return xk


# ── Static files ──

app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(WEB_DIR / "index.html"))


# ── Helpers ──

def _account_for_exchange(xk: str) -> dict:
    acct = AccountModel(env, xk)
    summary = acct.account_summary()
    pnl = acct.pnl()
    positions = acct.positions()
    unrealized = 0.0
    for sym, pos in positions.items():
        qty = float(pos.get("quantity", 0) or 0)
        cost_basis = float(pos.get("avg_cost_basis", 0) or 0)
        sell_price = float(pos.get("current_sell_price", 0) or 0)
        if qty > 0 and cost_basis > 0 and sell_price > 0:
            unrealized += (sell_price - cost_basis) * qty
    return {
        "exchange": xk,
        "account": summary,
        "pnl": {
            "total_realized_profit_usd": pnl.get("total_realized_profit_usd", 0),
            "unrealized_profit_usd": round(unrealized, 2),
            "lth_profit_bucket_usd": pnl.get("lth_profit_bucket_usd", 0),
        },
    }


def _resample(raw: list[dict], interval_s: int = 600) -> list[dict]:
    """Resample irregular account history to fixed time intervals via linear interpolation."""
    if len(raw) < 2:
        return raw
    t0 = raw[0]["ts"]
    t1 = raw[-1]["ts"]
    first_bin = int(t0 // interval_s) * interval_s + interval_s
    if first_bin > t1:
        return raw
    out = []
    j = 0
    t = first_bin
    while t <= t1:
        while j < len(raw) - 1 and raw[j + 1]["ts"] < t:
            j += 1
        a, b = raw[j], raw[j + 1] if j + 1 < len(raw) else raw[j]
        dt = b["ts"] - a["ts"]
        if dt > 0:
            frac = (t - a["ts"]) / dt
            val = a["total_account_value"] + frac * (b["total_account_value"] - a["total_account_value"])
        else:
            val = a["total_account_value"]
        out.append({"ts": t, "total_account_value": val})
        t += interval_s
    return out


# ── REST API ──

@app.get("/api/status")
async def api_status():
    env.reload()
    sm = SystemModel(env)
    ctrl_status = ctrl.status_summary()
    rr = sm.runner_ready()

    active = _active_exchanges()
    exchanges_data = {}
    for xk in active:
        exchanges_data[xk] = _account_for_exchange(xk)

    return {
        "system": {
            "neural_running": ctrl_status["neural_running"],
            "trader_running": ctrl_status["trader_running"],
            "data_manager_running": ctrl_status["data_manager_running"],
            "traders": ctrl_status.get("traders", {}),
            "runner_ready": rr.get("ready", False),
            "runner_stage": rr.get("stage", "unknown"),
            "any_training_running": ctrl_status["any_training_running"],
        },
        "exchanges": exchanges_data,
        "exchange_list": active,
        "trading_mode": env.trading_mode,
        "coins": env.coins,
    }


@app.get("/api/coins")
async def api_coins():
    env.reload()
    coins = []
    ctrl_status = ctrl.status_summary()

    active = _active_exchanges()
    positions_by_xk = {}
    for xk in active:
        acct = AccountModel(env, xk)
        positions_by_xk[xk] = acct.all_positions()

    for coin in env.coins:
        cm = CoinModel(env, coin)
        snap = cm.snapshot()

        snap["positions"] = {}
        mid_prices = []
        for xk in active:
            pos = positions_by_xk[xk].get(coin, {})
            snap["positions"][xk] = pos if pos.get("quantity", 0) > 0 else None
            buy = pos.get("current_buy_price", 0)
            sell = pos.get("current_sell_price", 0)
            mid = (buy + sell) / 2 if (buy and sell) else buy or sell
            if mid > 0:
                mid_prices.append(mid)

        snap["mid_price"] = mid_prices[0] if mid_prices else 0
        snap["training_running"] = ctrl_status["training"].get(coin, {}).get("running", False)
        fail = cm.training_failure()
        if fail and fail.get("exception_type"):
            snap["training_failure"] = fail
        coins.append(snap)
    return {"coins": coins}


@app.get("/api/coins/{coin}")
async def api_coin_detail(coin: str):
    coin = coin.upper()
    cm = CoinModel(env, coin)

    result = {**cm.snapshot(), "positions": {}, "trades": {}}
    for xk in _active_exchanges():
        acct = AccountModel(env, xk)
        positions = acct.all_positions()
        pos = positions.get(coin, {})
        result["positions"][xk] = pos if pos.get("quantity", 0) > 0 else None

        trades = acct.trade_history(limit=500)
        coin_trades = [t for t in trades if t.get("symbol", "").startswith(f"{coin}_")]
        result["trades"][xk] = coin_trades[-50:]

    return result


@app.get("/api/positions")
async def api_positions():
    out = {}
    dca = {}
    lth = {}
    for xk in _active_exchanges():
        acct = AccountModel(env, xk)
        out[xk] = acct.all_positions()
        dca[xk] = acct.dca_24h_by_coin()
        lth[xk] = acct.lth_holdings()
    return {"positions": out, "dca_24h": dca, "lth": lth}


@app.get("/api/trades")
async def api_trades(limit: int = 250, exchange: str = ""):
    result = {}
    targets = [exchange] if exchange else _active_exchanges()
    for xk in targets:
        acct = AccountModel(env, xk)
        result[xk] = acct.trade_history(limit=limit)
    return {"trades": result}


@app.get("/api/account-history")
async def api_account_history(hours: float = 0):
    """Return account value history for all exchanges, resampled to regular intervals."""
    result = {}
    if hours == 0:
        interval = 900        # 15 min for ALL
    elif hours <= 24:
        interval = 600        # 10 min
    elif hours <= 168:
        interval = 3600       # 1 hour
    else:
        interval = 14400      # 4 hours

    for xk in _active_exchanges():
        acct = AccountModel(env, xk)
        raw = acct.account_value_history(limit=0)
        if hours > 0:
            cutoff = time.time() - hours * 3600
            raw = [r for r in raw if r.get("ts", 0) >= cutoff]
        resampled = _resample(raw, interval) if raw else []
        if raw and resampled and raw[-1]["ts"] > resampled[-1]["ts"]:
            resampled.append(raw[-1])
        result[xk] = resampled
    return {"history": result}


@app.get("/api/comparison")
async def api_comparison():
    """Per-coin comparison across exchanges."""
    env.reload()
    active = _active_exchanges()
    coins_out = []
    for coin in env.coins:
        row = {"coin": coin}
        for xk in active:
            acct = AccountModel(env, xk)
            positions = acct.all_positions()
            pos = positions.get(coin, {})
            trades = acct.trade_history(limit=0)
            coin_trades = [t for t in trades if t.get("symbol", "").startswith(f"{coin}_")]
            total_fees = sum(float(t.get("fees_usd", 0) or 0) for t in coin_trades)
            pnl_pct = pos.get("gain_loss_pct_buy", 0) if pos.get("quantity", 0) > 0 else 0
            value = pos.get("value_usd", 0) if pos.get("quantity", 0) > 0 else 0
            row[xk] = {
                "value_usd": value,
                "pnl_pct": pnl_pct,
                "total_fees": total_fees,
                "trade_count": len(coin_trades),
            }
        coins_out.append(row)

    usdt_row = {"coin": "USDT"}
    for xk in active:
        acct = AccountModel(env, xk)
        summary = acct.account_summary()
        usdt_row[xk] = {
            "value_usd": summary.get("buying_power", 0),
            "pnl_pct": 0,
            "total_fees": 0,
            "trade_count": 0,
        }
    coins_out.append(usdt_row)

    coins_out.sort(key=lambda r: r["coin"])

    totals = {}
    for xk in active:
        acct = AccountModel(env, xk)
        pnl = acct.pnl()
        totals[xk] = {
            "realized_profit": pnl.get("total_realized_profit_usd", 0),
            "total_fees": sum(c[xk]["total_fees"] for c in coins_out),
        }

    return {"coins": coins_out, "totals": totals, "exchanges": active}


@app.get("/api/discovered-exchanges")
async def api_discovered_exchanges():
    from exchange_api import discover_exchanges
    return {"exchanges": discover_exchanges()}


@app.get("/api/config")
async def api_config():
    return env.get_config()


@app.get("/api/config/schema")
async def api_config_schema():
    from pt_env import CONFIG_SCHEMA
    return CONFIG_SCHEMA


@app.put("/api/config")
async def api_save_config(data: dict):
    try:
        env.set_config(data)
        return {"ok": True}
    except ValueError as e:
        return {"ok": False, "error": str(e)}


# Legacy aliases so any cached browser tabs still work
@app.get("/api/settings")
async def api_settings_compat():
    return await api_config()


@app.put("/api/settings")
async def api_save_settings_compat(data: dict):
    return await api_save_config(data)


@app.post("/api/start-all")
async def api_start_all():
    return ctrl.start_all()


@app.post("/api/stop-all")
async def api_stop_all():
    ctrl.stop_all()
    return {"ok": True}


@app.post("/api/data-manager/start")
async def api_data_manager_start():
    ok = ctrl.start_data_manager()
    return {"ok": ok}


@app.post("/api/data-manager/stop")
async def api_data_manager_stop():
    ctrl.stop_data_manager()
    return {"ok": True}


@app.get("/api/data-manager/stats")
async def api_data_manager_stats():
    """Per-coin stats from the local kucoin60 ArcticDB library."""
    try:
        import arcticdb as adb
        store = adb.Arctic(f"lmdb:///{env.historic_data_dir}")
        lib_name = "kucoin60"
        if lib_name not in store.list_libraries():
            return {"stats": {}, "error": None}
        lib = store.get_library(lib_name)
        symbols = set(lib.list_symbols())

        # Load error coins from status file if present
        status_path = env.hub_data_dir / "data_manager_status.json"
        error_coins: list = []
        if status_path.exists():
            try:
                error_coins = json.loads(status_path.read_text()).get("error_coins", [])
            except Exception:
                pass

        stats = {}
        now = pd.Timestamp.now(tz="UTC")
        for coin in env.coins:
            a_sym = f"{coin}_USDT"
            if coin in error_coins:
                stats[coin] = {"error": "Not available on KuCoin"}
                continue
            if a_sym not in symbols:
                stats[coin] = {"error": "No local data"}
                continue
            try:
                df = lib.read(a_sym, columns=[]).data
                last_ts = df.index[-1]
                age_minutes = int((now - last_ts).total_seconds() / 60)
                stats[coin] = {
                    "rows": len(df),
                    "first": str(df.index[0].date()),
                    "last": str(last_ts),
                    "age_minutes": age_minutes,
                    "error": None,
                }
            except Exception as e:
                stats[coin] = {"error": str(e)}
        return {"stats": stats, "error": None}
    except Exception as e:
        return {"stats": {}, "error": str(e)}


@app.post("/api/start-neural")
async def api_start_neural():
    ok = ctrl.start_neural()
    return {"ok": ok}


@app.post("/api/stop-neural")
async def api_stop_neural():
    ctrl.stop_neural()
    return {"ok": True}


@app.post("/api/start-trader")
async def api_start_trader():
    ok = ctrl.start_trader()
    return {"ok": ok}


@app.post("/api/stop-trader")
async def api_stop_trader():
    ctrl.stop_trader()
    return {"ok": True}


@app.post("/api/start-trader/{exchange}")
async def api_start_trader_xk(exchange: str):
    ok = ctrl.start_trader(exchange.lower())
    return {"ok": ok}


@app.post("/api/stop-trader/{exchange}")
async def api_stop_trader_xk(exchange: str):
    ctrl.stop_trader(exchange.lower())
    return {"ok": True}


@app.post("/api/train-all")
async def api_train_all():
    results = ctrl.train_all()
    return {"ok": True, "results": results}


@app.post("/api/train/{coin}")
async def api_train_coin(coin: str):
    ok = ctrl.start_training(coin.upper())
    return {"ok": ok}


@app.post("/api/stop-training/{coin}")
async def api_stop_training(coin: str):
    ctrl.stop_training(coin.upper())
    return {"ok": True}


@app.get("/api/logs/{script}")
async def api_logs(script: str, limit: int = 200):
    lines = ctrl.peek_logs(script, limit)
    return {"lines": lines}


def _refresh_exchange_balance(xk: str, write_history: bool = True):
    """Query adapter for current balance and update trader_status file."""
    adapter = _get_adapter(xk)
    if not adapter:
        return
    try:
        total_value = adapter.get_account_value() or 0
        buying_power = adapter.get_buying_power() or 0
        holdings_value = total_value - buying_power
        pct = (holdings_value / total_value * 100) if total_value > 0 else 0

        status_path = env.trader_status_path(xk)
        existing = {}
        if status_path.exists():
            try:
                with open(status_path) as f:
                    existing = json.load(f)
            except Exception:
                pass

        existing.setdefault("account", {}).update({
            "total_account_value": total_value,
            "buying_power": buying_power,
            "holdings_sell_value": holdings_value,
            "holdings_buy_value": holdings_value,
            "percent_in_trade": pct,
        })
        existing.setdefault("positions", {})

        status_path.parent.mkdir(parents=True, exist_ok=True)
        with open(status_path, "w") as f:
            json.dump(existing, f)

        if write_history and total_value > 0:
            hist_path = env.account_history_path(xk)
            with open(hist_path, "a") as f:
                f.write(json.dumps({"ts": time.time(), "total_account_value": total_value}) + "\n")
    except Exception as e:
        print(f"[Balance] {xk} refresh failed: {e}")


@app.post("/api/clear-account-history")
async def api_clear_account_history():
    """Delete account value history files for all exchanges."""
    for xk in _active_exchanges():
        path = env.account_history_path(xk)
        try:
            if path.exists():
                path.write_text("")
        except Exception:
            pass
    return {"ok": True}


@app.post("/api/reset-all")
async def api_reset_all():
    """Stop everything, close all positions, wipe all state, reset to sync-source USD balance."""
    ctrl.stop_all()
    active = _active_exchanges()

    for xk in active:
        adapter = _get_adapter(xk)
        if adapter and xk not in ("control", "demo"):
            for coin, qty in adapter.get_holdings().items():
                try:
                    adapter.place_sell(f"{coin}_USD", qty)
                except Exception:
                    pass

    sync_xk = _control_sync_exchange()
    adapter_sync = _get_adapter(sync_xk) if sync_xk else None
    balance = adapter_sync.get_buying_power() if adapter_sync else 0

    for xk in active:
        for path in [env.trade_history_path(xk), env.account_history_path(xk)]:
            try:
                path.write_text("")
            except Exception:
                pass
        try:
            env.pnl_ledger_path(xk).write_text(json.dumps({
                "total_realized_profit_usd": 0.0,
                "open_positions": {},
                "lth_profit_bucket_usd": 0.0,
            }, indent=2))
        except Exception:
            pass
        try:
            env.bot_order_ids_path(xk).write_text("{}")
        except Exception:
            pass
        try:
            env.trader_status_path(xk).write_text(json.dumps(
                {"account": {}, "positions": {}}, indent=2
            ))
        except Exception:
            pass

    ctrl_state = env.exchange_state_path(_ctrl_xk())
    try:
        ctrl_state.write_text(json.dumps({
            "usd_balance": balance or 0,
            "holdings": {},
            "orders": {},
        }, indent=2))
    except Exception:
        pass

    _adapters.pop(_ctrl_xk(), None)
    _get_mirror().reload()
    for xk in _active_exchanges():
        _refresh_exchange_balance(xk)

    return {"ok": True, "balance": balance}


@app.post("/api/close-all")
async def api_close_all():
    """Sell all positions on all real exchanges, return to USDT."""
    ctrl.stop_trader()
    results = {}
    for xk in env.exchanges:  # real exchanges only
        adapter = _get_adapter(xk)
        if not adapter:
            results[xk] = {"ok": False, "error": "no adapter"}
            continue
        holdings = adapter.get_holdings()
        trades = []
        for coin, qty in holdings.items():
            symbol = f"{coin}_USD"
            result = adapter.place_sell(symbol, qty)
            sold = result is not None
            trades.append({"coin": coin, "qty": qty, "sold": sold})
            if sold:
                _record_close_trade(xk, coin, symbol, qty, result)
                _get_mirror().mirror_sell(coin, tag="CLOSE_ALL")
        _refresh_exchange_balance(xk)
        _clear_trader_positions(xk)
        results[xk] = {"ok": True, "trades": trades}
    _clear_trader_positions(_ctrl_xk())
    return {"ok": True, "results": results}


@app.post("/api/close-coin/{coin}/{exchange}")
async def api_close_coin(coin: str, exchange: str):
    """Sell a single coin's position on a specific exchange."""
    coin = coin.upper()
    xk = exchange.lower()

    if xk in ("control", "demo"):
        return {"ok": False, "error": "Control/demo positions close automatically when the real exchange closes"}

    if xk not in env.exchanges:
        return {"ok": False, "error": f"Unknown exchange: {xk}"}

    adapter = _get_adapter(xk)
    if not adapter:
        return {"ok": False, "error": "No adapter"}

    holdings = adapter.get_holdings()
    qty = holdings.get(coin, 0)
    symbol = f"{coin}_USD"

    if qty > 0:
        result = adapter.place_sell(symbol, qty)
        if not result:
            return {"ok": False, "error": "Sell failed"}
        _record_close_trade(xk, coin, symbol, qty, result, tag="CLOSE")
        _get_mirror().mirror_sell(coin, tag="CLOSE")
    else:
        ledger = {}
        try:
            lp = env.pnl_ledger_path(xk)
            if lp.exists():
                ledger = json.loads(lp.read_text())
        except Exception:
            pass
        has_ledger = float(
            (ledger.get("open_positions") or {}).get(coin, {}).get("qty", 0) or 0
        ) > 1e-12
        status = {}
        try:
            sp = env.trader_status_path(xk)
            if sp.exists():
                status = json.loads(sp.read_text())
        except Exception:
            pass
        has_status = float(
            (status.get("positions") or {}).get(coin, {}).get("quantity", 0) or 0
        ) > 1e-12
        if not has_ledger and not has_status:
            return {"ok": False, "error": f"No {coin} position on {xk}"}
        if has_ledger:
            (ledger.get("open_positions") or {}).pop(coin, None)
            try:
                env.pnl_ledger_path(xk).write_text(json.dumps(ledger, indent=2))
            except Exception:
                pass
        _get_mirror().mirror_sell(coin, tag="CLOSE")

    _clear_coin_position(xk, coin)
    _clear_coin_position(_ctrl_xk(), coin)
    _refresh_exchange_balance(xk)

    return {"ok": True, "coin": coin, "exchange": xk, "qty": qty}


def _clear_coin_position(xk: str, coin: str):
    """Zero out a single coin's position in trader_status file."""
    path = env.trader_status_path(xk)
    try:
        data = json.loads(path.read_text()) if path.exists() else {}
        pos = (data.get("positions") or {}).get(coin)
        if isinstance(pos, dict):
            pos["quantity"] = 0
            pos["value_usd"] = 0
            pos["avg_cost_basis"] = 0
            pos["gain_loss_pct_buy"] = 0
            pos["gain_loss_pct_sell"] = 0
        path.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def _clear_trader_positions(xk: str):
    """Zero out positions in trader_status file after close-all."""
    path = env.trader_status_path(xk)
    try:
        data = json.loads(path.read_text()) if path.exists() else {}
        for pos in (data.get("positions") or {}).values():
            if isinstance(pos, dict):
                pos["quantity"] = 0
                pos["value_usd"] = 0
                pos["avg_cost_basis"] = 0
                pos["gain_loss_pct_buy"] = 0
                pos["gain_loss_pct_sell"] = 0
        path.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def _record_close_trade(xk: str, coin: str, symbol: str, qty: float, result,
                        tag: str = "CLOSE_ALL"):
    """Record a close sell in trade history and update PnL ledger."""
    ts = time.time()
    price = result.avg_price
    notional = result.notional_usd or (price * qty if price else None)
    fees = result.fees_usd

    ledger_path = env.pnl_ledger_path(xk)
    try:
        ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
    except Exception:
        ledger = {}

    pos = (ledger.get("open_positions") or {}).get(coin)
    pos_qty = float(pos.get("qty", 0) or 0) if isinstance(pos, dict) else 0.0
    pos_cost = float(pos.get("usd_cost", 0) or 0) if isinstance(pos, dict) else 0.0

    frac = min(1.0, qty / pos_qty) if pos_qty > 0 else 1.0
    cost_used = pos_cost * frac
    avg_cost = cost_used / qty if qty > 0 else None

    realized = None
    if notional is not None:
        fee_val = float(fees) if fees is not None else 0.0
        realized = (notional - fee_val) - cost_used

    pnl_pct = None
    if realized is not None and cost_used > 0:
        pnl_pct = (realized / cost_used) * 100

    if isinstance(pos, dict):
        pos["usd_cost"] = pos_cost - cost_used
        pos["qty"] = pos_qty - qty
        if float(pos.get("qty", 0) or 0) <= 1e-12:
            ledger.get("open_positions", {}).pop(coin, None)

    if realized is not None:
        ledger["total_realized_profit_usd"] = float(
            ledger.get("total_realized_profit_usd", 0) or 0
        ) + realized

    try:
        ledger_path.write_text(json.dumps(ledger, indent=2))
    except Exception:
        pass

    entry = {
        "ts": ts,
        "side": "sell",
        "tag": tag,
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "notional_usd": notional,
        "net_usd": (notional - (float(fees) if fees else 0.0)) if notional else None,
        "avg_cost_basis": avg_cost,
        "pnl_pct": pnl_pct,
        "fees_usd": fees,
        "fees_missing": fees is None,
        "fees_fallback_applied_usd": 0.0,
        "realized_profit_usd": realized,
        "order_id": result.order_id,
        "position_cost_used_usd": cost_used,
        "position_cost_after_usd": float((ledger.get("open_positions") or {}).get(coin, {}).get("usd_cost", 0) or 0),
    }
    history_path = env.trade_history_path(xk)
    try:
        with open(history_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        pass


@app.post("/api/sync-control")
async def api_sync_control():
    """Reset control/demo balance to match the configured sync-source exchange. Requires traders stopped, no positions."""
    if ctrl.trader_running:
        return {"ok": False, "error": "Traders must be stopped"}

    for xk in env.exchanges:
        a = _get_adapter(xk)
        if a:
            holdings = a.get_holdings()
            pnl = json.loads(env.pnl_ledger_path(xk).read_text()) if env.pnl_ledger_path(xk).exists() else {}
            open_pos = pnl.get("open_positions") or {}
            has_real = any(
                float((open_pos.get(coin) or {}).get("qty", 0) or 0) > 1e-12
                for coin in holdings
            )
            if has_real:
                return {"ok": False, "error": f"{xk} has open positions"}

    sync_xk = _control_sync_exchange()
    adapter_sync = _get_adapter(sync_xk) if sync_xk else None
    if not adapter_sync:
        return {"ok": False, "error": f"Cannot connect to sync exchange ({sync_xk or 'none configured'})"}

    buying_power = adapter_sync.get_buying_power()
    if not buying_power or buying_power <= 0:
        return {"ok": False, "error": f"{sync_xk} has no USD balance"}

    ck = _ctrl_xk()
    state_path = env.exchange_state_path(ck)
    with open(state_path, "w") as f:
        json.dump({"usd_balance": buying_power, "holdings": {}, "orders": {}}, f, indent=2)

    _adapters.pop(ck, None)
    _get_mirror().reload()
    _refresh_exchange_balance(ck)
    print(f"[Sync] {ck} balance set to ${buying_power:,.2f} from {sync_xk}")
    return {"ok": True, "balance": buying_power}


@app.get("/api/candles/{coin}")
async def api_candles(coin: str, timeframe: str = "1hour", limit: int = 250,
                      source: str = ""):
    """Fetch candle data for charting. Source: kraken or kucoin (auto from settings)."""
    coin = coin.upper()
    env.reload()
    price_source = source or env.get_config()["live_price_source"]

    if price_source == "kraken":
        return _candles_kraken(coin, timeframe, limit)
    return _candles_kucoin(coin, timeframe, limit)


def _candles_kraken(coin: str, timeframe: str, limit: int) -> dict:
    tf_map = {
        "1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m",
        "1hour": "1h", "2hour": "2h", "4hour": "4h",
        "8hour": "8h", "12hour": "12h", "1day": "1d", "1week": "1w",
    }
    ccxt_tf = tf_map.get(timeframe, "1h")
    try:
        import ccxt
        kraken = ccxt.kraken({"enableRateLimit": True})
        ohlcv = kraken.fetch_ohlcv(f"{coin}/USDT", ccxt_tf, limit=limit)
        candles = []
        for bar in ohlcv:
            candles.append({
                "time": int(bar[0] / 1000),
                "open": float(bar[1]),
                "high": float(bar[2]),
                "low": float(bar[3]),
                "close": float(bar[4]),
                "volume": float(bar[5]),
            })
        return {"candles": candles}
    except Exception as e:
        return _candles_kucoin(coin, timeframe, limit)


def _candles_kucoin(coin: str, timeframe: str, limit: int) -> dict:
    tf_map = {
        "1min": "1min", "5min": "5min", "15min": "15min", "30min": "30min",
        "1hour": "1hour", "2hour": "2hour", "4hour": "4hour",
        "8hour": "8hour", "12hour": "12hour", "1day": "1day", "1week": "1week",
    }
    kc_tf = tf_map.get(timeframe, "1hour")
    try:
        import requests as _req
        url = f"https://api.kucoin.com/api/v1/market/candles?type={kc_tf}&symbol={coin}-USDT&pageSize={limit}"
        resp = _req.get(url, timeout=10)
        data = resp.json()
        klines = data.get("data", [])
        candles = []
        for k in reversed(klines):
            candles.append({
                "time": int(k[0]),
                "open": float(k[1]),
                "close": float(k[2]),
                "high": float(k[3]),
                "low": float(k[4]),
                "volume": float(k[5]),
            })
        return {"candles": candles}
    except Exception as e:
        return {"candles": [], "error": str(e)}


# ── WebSocket for real-time updates ──

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


async def _file_watcher():
    """Poll key files and broadcast changes to WebSocket clients."""
    mtimes: dict[str, float] = {}
    balance_tick = 0

    def _check(path: Path, key: str) -> bool:
        try:
            mt = path.stat().st_mtime
        except FileNotFoundError:
            return False
        old = mtimes.get(key, 0)
        if mt != old:
            mtimes[key] = mt
            return True
        return False

    while True:
        if not ws_manager.active:
            await asyncio.sleep(2)
            continue

        try:
            env.reload()

            for xk in _active_exchanges():
                if _check(env.trader_status_path(xk), f"trader_status_{xk}"):
                    acct = AccountModel(env, xk)
                    ts = acct.trader_status()
                    if ts:
                        await ws_manager.broadcast({
                            "type": "trader_status",
                            "exchange": xk,
                            "data": ts,
                        })
                        xd = _account_for_exchange(xk)
                        await ws_manager.broadcast({
                            "type": "pnl",
                            "exchange": xk,
                            "data": xd["pnl"],
                        })

                if _check(env.pnl_ledger_path(xk), f"pnl_{xk}"):
                    xd = _account_for_exchange(xk)
                    await ws_manager.broadcast({
                        "type": "pnl",
                        "exchange": xk,
                        "data": xd["pnl"],
                    })

            signals_changed = False
            for coin in env.coins:
                if _check(env.long_signal_path(coin), f"long_{coin}"):
                    signals_changed = True
                if _check(env.short_signal_path(coin), f"short_{coin}"):
                    signals_changed = True

            if signals_changed:
                coins_data = {}
                for coin in env.coins:
                    cm = CoinModel(env, coin)
                    coins_data[coin] = {
                        "long": cm.long_signal(),
                        "short": cm.short_signal(),
                        "long_prices": cm.long_price_levels(),
                        "short_prices": cm.short_price_levels(),
                    }
                await ws_manager.broadcast({"type": "signals", "data": coins_data})

            if _check(env.runner_ready_path(), "runner_ready"):
                sm = SystemModel(env)
                rr = sm.runner_ready()
                await ws_manager.broadcast({"type": "runner_ready", "data": rr})
                if rr.get("ready") and ctrl.neural_running:
                    ctrl.poll_ready_and_start_trader()

            dm_status_path = env.hub_data_dir / "data_manager_status.json"
            if _check(dm_status_path, "data_manager_status"):
                try:
                    dm_data = json.loads(dm_status_path.read_text())
                except Exception:
                    dm_data = {}
                await ws_manager.broadcast({"type": "data_manager_status", "data": dm_data})

            traders_status = {}
            for xk in _active_exchanges():
                traders_status[xk] = {"running": ctrl.trader_running_for(xk)}
            sys_status = {
                "neural_running": ctrl.neural_running,
                "trader_running": ctrl.trader_running,
                "data_manager_running": ctrl.data_manager_running,
                "traders": traders_status,
                "any_training_running": ctrl.any_training_running(),
            }
            await ws_manager.broadcast({"type": "system", "data": sys_status})

            balance_tick += 1
            if balance_tick >= 20:
                balance_tick = 0
                try:
                    _get_mirror().write_status()
                except Exception:
                    pass
                for xk in _active_exchanges():
                    if not ctrl.trader_running_for(xk):
                        _refresh_exchange_balance(xk, write_history=False)

        except Exception:
            pass

        await asyncio.sleep(1.5)


def _init_exchange_balances():
    """Seed initial control state if needed, then refresh all balances."""
    env.reload()

    ck = _ctrl_xk()
    ctrl_state = env.exchange_state_path(ck)
    if not ctrl_state.exists():
        starting = float(env.get_config().get("control_starting_usd") or 0)
        if starting <= 0:
            sync_xk = _control_sync_exchange()
            if sync_xk:
                adapter_sync = _get_adapter(sync_xk)
                starting = (adapter_sync.get_buying_power() or 0) if adapter_sync else 0
                print(f"[Init] {ck} starting balance from {sync_xk}: ${starting:,.2f}")
        if starting > 0:
            with open(ctrl_state, "w") as f:
                json.dump({"usd_balance": starting, "holdings": {}, "orders": {}}, f)
            print(f"[Init] wrote {ctrl_state}")

    for xk in _active_exchanges():
        _refresh_exchange_balance(xk)
        status_path = env.trader_status_path(xk)
        if status_path.exists():
            try:
                with open(status_path) as f:
                    data = json.load(f)
                total = data.get("account", {}).get("total_account_value", 0)
                print(f"[Init] {xk}: ${total:,.2f}")
            except Exception:
                pass


def _migrate_hub_data_flat_to_subdirs():
    """One-time migration: move flat hub_data/{name}_{xk}.ext into hub_data/exchanges/{xk}/{name}.ext."""
    import re
    hub = env.hub_data_dir
    if not hub.exists():
        return
    patterns = [
        (re.compile(r'^trader_status_(.+)\.json$'),            "trader_status.json"),
        (re.compile(r'^trade_history_(.+)\.jsonl$'),           "trade_history.jsonl"),
        (re.compile(r'^pnl_ledger_(.+)\.json$'),               "pnl_ledger.json"),
        (re.compile(r'^account_value_history_(.+)\.jsonl$'),   "account_value_history.jsonl"),
        (re.compile(r'^bot_order_ids_(.+)\.json$'),            "bot_order_ids.json"),
        (re.compile(r'^(.+)_exchange_state\.json$'),           "exchange_state.json"),
    ]
    moved = 0
    for f in sorted(hub.iterdir()):
        if not f.is_file():
            continue
        for pattern, new_name in patterns:
            m = pattern.match(f.name)
            if m:
                xk = m.group(1)
                dest_dir = hub / "exchanges" / xk
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest = dest_dir / new_name
                if not dest.exists():
                    f.rename(dest)
                    print(f"[Migrate] {f.name} → exchanges/{xk}/{new_name}")
                    moved += 1
                break
    if moved:
        print(f"[Migrate] Moved {moved} state files into subdirectories")


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    _migrate_hub_data_flat_to_subdirs()
    _init_exchange_balances()
    asyncio.create_task(_file_watcher())
    yield

app.router.lifespan_context = lifespan


# ── Entry point ──

if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser(description="PowerTrader Web UI")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
