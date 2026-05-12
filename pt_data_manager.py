"""
PowerTrader local KuCoin data manager.

Launched as a subprocess by pt_controller. On start:
  - For each coin: backfill if no local data exists, topup if data exists.
  - Enters Normal (idle) state, then topups at a fixed interval derived
    from kucoin_local_topup_hours config (e.g. [0,6,12,18] → every 6h).

Writes hub_data/data_manager_status.json at every state transition so the
web UI can display the current state without polling logs.

State values: Stopped | Backfill | Topup | Normal
"""

import json
import os
import sys
import time
import traceback

import ccxt
import pandas as pd

from pt_env import PTEnv

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TF_MINUTES = [60, 120, 240, 480, 720, 1440, 10080]

CCXT_TF = {
    60: "1h", 120: "2h", 240: "4h", 480: "8h",
    720: "12h", 1440: "1d", 10080: "1w",
}

_env = PTEnv(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lib_name(tf: int) -> str:
    return f"kucoin{tf}"


def _arctic_sym(coin: str) -> str:
    return f"{coin}_USDT"


def _ccxt_sym(coin: str) -> str:
    return f"{coin}/USDT"


def _get_arctic():
    import arcticdb as adb
    data_dir = _env.historic_data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    return adb.Arctic(f"lmdb:///{data_dir}")


def _log(msg: str):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def _write_status(state: str, coin: str = "", tf: int = 0,
                  error_coins: list = None, last_topup: float = 0.0):
    status = {
        "state": state,
        "coin": coin,
        "tf_minutes": tf,
        "last_topup": last_topup,
        "error_coins": error_coins or [],
        "ts": time.time(),
    }
    path = _env.hub_data_dir / "data_manager_status.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(status, indent=2))
        tmp.rename(path)
    except Exception as e:
        _log(f"[Status] write failed: {e}")


def _topup_interval_seconds(hours: list) -> float:
    """Derive sleep interval from configured hours list.

    Sorts hours, finds minimum gap between consecutive entries (wrapping
    midnight), returns that gap in seconds.  Falls back to 6h if list is
    empty or has one entry.
    """
    if not hours or len(hours) < 2:
        return 6 * 3600
    s = sorted(set(int(h) for h in hours))
    gaps = [s[i + 1] - s[i] for i in range(len(s) - 1)]
    gaps.append(24 - s[-1] + s[0])  # wraparound gap
    return min(gaps) * 3600


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def _backfill_coin(client, arctic, coin: str, error_coins: list):
    """Backfill all timeframes for a single coin. Logs errors per TF."""
    a_sym = _arctic_sym(coin)
    c_sym = _ccxt_sym(coin)

    if c_sym not in client.markets:
        _log(f"[Backfill] {coin}: not available on KuCoin — skipping")
        if coin not in error_coins:
            error_coins.append(coin)
        return

    for tf in TF_MINUTES:
        lib_name = _lib_name(tf)
        ccxt_tf = CCXT_TF[tf]
        tf_ms = tf * 60 * 1000

        if lib_name not in arctic.list_libraries():
            arctic.create_library(lib_name)
        lib = arctic.get_library(lib_name)

        if a_sym in lib.list_symbols():
            continue  # already have data; topup handles this

        _log(f"[Backfill] {coin} {lib_name} …")
        try:
            all_candles = []
            end_ms = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)

            while True:
                since_ms = end_ms - (1500 * tf_ms)
                candles = client.fetch_ohlcv(c_sym, ccxt_tf, since=since_ms, limit=1500)
                time.sleep(0.4)
                if not candles:
                    break
                all_candles.extend(candles)
                oldest = candles[0][0]
                if oldest >= since_ms + tf_ms:
                    break
                end_ms = oldest - 1

            if not all_candles:
                _log(f"[Backfill] {coin} {lib_name}: no candles returned")
                continue

            seen: set = set()
            deduped = [c for c in all_candles if not (c[0] in seen or seen.add(c[0]))]

            df = pd.DataFrame(deduped,
                              columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.set_index("timestamp").sort_index().astype(float)

            lib.write(a_sym, df, prune_previous_versions=True)
            _log(f"[Backfill] {coin} {lib_name}: {len(df)} rows "
                 f"({df.index[0].date()} → {df.index[-1].date()})")

        except Exception as e:
            _log(f"[Backfill] {coin} {lib_name} error: {e}")
            traceback.print_exc()


# ---------------------------------------------------------------------------
# Topup
# ---------------------------------------------------------------------------

def _topup_coin(client, arctic, coin: str, error_coins: list):
    """Topup all timeframes for a single coin that already has data."""
    a_sym = _arctic_sym(coin)
    c_sym = _ccxt_sym(coin)

    if c_sym not in client.markets:
        _log(f"[Topup] {coin}: not available on KuCoin")
        if coin not in error_coins:
            error_coins.append(coin)
        return

    for tf in TF_MINUTES:
        lib_name = _lib_name(tf)
        ccxt_tf = CCXT_TF[tf]

        if lib_name not in arctic.list_libraries():
            continue
        lib = arctic.get_library(lib_name)
        if a_sym not in lib.list_symbols():
            continue

        try:
            last_ts = lib.read(a_sym, columns=[]).data.index[-1]
            since_ms = int(last_ts.timestamp() * 1000) + 1

            all_candles = []
            seen_ts: set = set()
            while True:
                candles = client.fetch_ohlcv(c_sym, ccxt_tf, since=since_ms, limit=1500)
                time.sleep(0.4)
                if not candles:
                    break
                new = [c for c in candles if c[0] not in seen_ts]
                if not new:
                    break
                all_candles.extend(new)
                seen_ts.update(c[0] for c in new)
                since_ms = candles[-1][0] + 1

            if not all_candles:
                _log(f"[Topup] {coin} {lib_name}: up to date")
                continue

            df = pd.DataFrame(all_candles,
                              columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.set_index("timestamp").sort_index().astype(float)
            df = df[df.index > last_ts]

            if df.empty:
                _log(f"[Topup] {coin} {lib_name}: up to date")
                continue

            lib.append(a_sym, df)
            _log(f"[Topup] {coin} {lib_name}: +{len(df)} rows → {df.index[-1]}")

        except Exception as e:
            _log(f"[Topup] {coin} {lib_name} error: {e}")
            traceback.print_exc()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _get_client():
    client = ccxt.kucoin({"enableRateLimit": True})
    client.timeout = 30000
    client.load_markets()
    return client


def run():
    _log("[DataManager] starting")
    error_coins: list = []

    # ── Initial pass: backfill missing, topup existing ────────────────────
    cfg = _env.get_config()
    coins: list[str] = [str(c).upper() for c in cfg.get("coins", []) if str(c).strip()]
    topup_hours = cfg.get("kucoin_local_topup_hours", [0, 6, 12, 18])
    interval_s = _topup_interval_seconds(topup_hours)

    _log(f"[DataManager] {len(coins)} coins, topup every {interval_s/3600:.1f}h")

    try:
        client = _get_client()
        arctic = _get_arctic()
    except Exception as e:
        _log(f"[DataManager] startup failed: {e}")
        _write_status("Normal", error_coins=["startup failed"])
        return

    # Categorise coins
    coins_to_backfill = []
    coins_to_topup = []
    for coin in coins:
        a_sym = _arctic_sym(coin)
        lib60 = _lib_name(60)
        try:
            if lib60 in arctic.list_libraries():
                lib = arctic.get_library(lib60)
                if a_sym in lib.list_symbols():
                    coins_to_topup.append(coin)
                    continue
        except Exception:
            pass
        coins_to_backfill.append(coin)

    _log(f"[DataManager] {len(coins_to_backfill)} to backfill, "
         f"{len(coins_to_topup)} to topup")

    # Backfill
    for coin in coins_to_backfill:
        _write_status("Backfill", coin=coin, error_coins=error_coins)
        _backfill_coin(client, arctic, coin, error_coins)

    # Topup
    for coin in coins_to_topup:
        _write_status("Topup", coin=coin, error_coins=error_coins)
        _topup_coin(client, arctic, coin, error_coins)

    if error_coins:
        _log(f"[DataManager] problem coins: {', '.join(error_coins)}")

    last_topup = time.time()
    _write_status("Normal", error_coins=error_coins, last_topup=last_topup)
    _log(f"[DataManager] initial pass complete — next topup in {interval_s/3600:.1f}h")

    # ── Scheduled topup loop ──────────────────────────────────────────────
    while True:
        time.sleep(60)  # wake every minute to check if interval elapsed

        # Re-read config each cycle so changes are picked up
        cfg = _env.get_config()
        coins = [str(c).upper() for c in cfg.get("coins", []) if str(c).strip()]
        topup_hours = cfg.get("kucoin_local_topup_hours", [0, 6, 12, 18])
        interval_s = _topup_interval_seconds(topup_hours)

        if time.time() - last_topup < interval_s:
            continue

        _log("[DataManager] scheduled topup starting")
        error_coins = []
        try:
            client = _get_client()
            arctic = _get_arctic()
        except Exception as e:
            _log(f"[DataManager] client error: {e}")
            continue

        for coin in coins:
            _write_status("Topup", coin=coin, error_coins=error_coins)
            a_sym = _arctic_sym(coin)
            lib60 = _lib_name(60)
            try:
                has_data = (lib60 in arctic.list_libraries() and
                            a_sym in arctic.get_library(lib60).list_symbols())
            except Exception:
                has_data = False

            if has_data:
                _topup_coin(client, arctic, coin, error_coins)
            else:
                _backfill_coin(client, arctic, coin, error_coins)

        last_topup = time.time()
        _write_status("Normal", error_coins=error_coins, last_topup=last_topup)
        _log(f"[DataManager] topup complete — next in {interval_s/3600:.1f}h")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        _log("[DataManager] stopped")
    except Exception as e:
        _log(f"[DataManager] fatal: {e}")
        traceback.print_exc()
    finally:
        _write_status("Stopped")
