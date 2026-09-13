"""Shared error bus for PowerTrader_AI.

Any process (thinker, trader, exchange adapters, controller) calls emit() to
record a structured error/warning to hub_data/errors.jsonl. The web UI reads
this file and displays a filterable error log.

Each write is a single JSON line append — atomic on Linux for payloads under
~4 KB, so multiple processes can write concurrently without corruption.

Also provides retry() — a shared poll/backoff helper for flaky external
calls (price fetches, order status polling) that never resolves a failure
by returning None; it always either succeeds or raises, so callers can
report the failure via emit() with call-site context.
"""

import json
import os
import time
from datetime import datetime
from pt_env import utcnow

_path_cache: str | None = None


def _errors_path() -> str:
    global _path_cache
    if _path_cache is None:
        from pt_env import PTEnv
        env = PTEnv(os.path.dirname(os.path.abspath(__file__)))
        _path_cache = str(env.errors_path())
    return _path_cache


def emit(component: str, message: str, level: str = "error", detail: str = "") -> None:
    """Log a structured error/warning to stdout and to errors.jsonl."""
    _now = datetime.now()
    _ts = _now.strftime("%Y%m%d:%H%M%S") + f".{_now.microsecond // 1000:03d}"
    print(f"{_ts} {level.upper():<8} [{component}] {message}", flush=True)
    try:
        path = _errors_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        entry = json.dumps({
            "ts": utcnow(),
            "component": component,
            "level": level,
            "message": message,
            "detail": detail,
        })
        with open(path, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
    except Exception:
        pass  # Never fail trying to log

    try:
        import pt_notify
        pt_notify.notify_error(component, level, message, detail)
    except Exception:
        pass


class RetryExhausted(Exception):
    """Raised by retry() when every attempt ran without ever raising, but
    none produced a result is_success() accepted (e.g. a poll that kept
    returning a non-terminal state until the timeout ran out)."""


def retry(
    func,
    is_success=lambda result: True,
    timeout: float = 60.0,
    start_interval: float = 1.0,
    interval_ramp: float = 1.5,
):
    """Call func() until it returns a value is_success() accepts, or until
    timeout seconds have elapsed since the first attempt — whichever comes
    first. Retries on either a raised exception or a rejected result.

    One helper covers both shapes callers need: "retry on exception" (leave
    is_success at its default, so the first non-raising result wins) and
    "poll until condition" (e.g. an order reaching a terminal state) via
    is_success. Bounding by wall-clock time rather than an attempt count
    means the total time a caller can be blocked is always known up front,
    regardless of interval_ramp or how long each attempt itself takes.

    Never resolves a failure by returning None. On timeout it raises — the
    last exception if func() ever raised one, otherwise RetryExhausted —
    so a caller can't mistake "gave up" for "nothing to report". Add
    call-site context (which symbol, which order) in the caller's except
    block, then emit() it.

    Waits start_interval seconds after the first failed attempt, then
    multiplies the wait by interval_ramp after each subsequent one (capped
    so it never sleeps past the deadline), so a flaky call backs off
    instead of hammering the API. Pass interval_ramp=1.0 for a
    fixed-cadence poll.
    """
    deadline = time.time() + timeout
    interval = start_interval
    last_exc: Exception | None = None
    last_result = None
    got_result = False

    while True:
        try:
            result = func()
        except Exception as e:
            last_exc = e
        else:
            last_exc = None
            last_result = result
            got_result = True
            if is_success(result):
                return result

        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(interval, remaining))
        interval *= interval_ramp

    if last_exc is not None:
        raise last_exc
    raise RetryExhausted(
        f"retry() gave up after timeout={timeout}s without a successful result"
        + (f" (last result: {last_result!r})" if got_result else "")
    )


def trim(max_lines: int = 500) -> None:
    """Trim errors.jsonl to the most recent max_lines entries. Called at web startup."""
    try:
        path = _errors_path()
        if not os.path.isfile(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) <= max_lines:
            return
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines[-max_lines:])
    except Exception:
        pass
