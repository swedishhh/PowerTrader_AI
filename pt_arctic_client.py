"""
Client for the singleton ArcticDB server (see pt_arctic_server.py).

Connects over a local Unix socket; the returned proxy exposes the same
methods as pt_arctic_server.ArcticService (list_libraries, list_symbols,
read, write, append, create_library) and is a drop-in backend for
ArcticPriceSource or any other live-path Arctic reader/writer in this repo.

Only for the live container's own store (state/historic_data). Backtest and
other local-dev code keeps opening ArcticDB directly via ArcticService /
ArcticPriceSource's default arctic_url — there's no cross-process contention
to worry about there, so no reason to route it through this server.
"""

from __future__ import annotations

import os
import time

from pt_arctic_server import AUTHKEY, SOCKET_PATH, _ArcticManager

_RECONNECT_ERRORS = (EOFError, ConnectionResetError, ConnectionRefusedError,
                     BrokenPipeError, OSError)


def connect(socket_path: str = SOCKET_PATH, timeout: float = 15.0,
            retry_interval: float = 0.25):
    """Connect to the running Arctic singleton, retrying until `timeout`
    seconds have elapsed (the server may still be starting up, or may have
    just been restarted after a crash)."""
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        manager = _ArcticManager(address=socket_path, authkey=AUTHKEY)
        try:
            manager.connect()
            return manager.get_service()
        except _RECONNECT_ERRORS as e:
            last_err = e
            time.sleep(retry_interval)
    raise ConnectionError(
        f"Could not connect to Arctic server at {socket_path!r} after {timeout}s: {last_err}"
    )


class _ResilientBackend:
    """Thin lazy-connecting wrapper: reconnects and retries once if the
    underlying proxy call fails with a connection error (e.g. the server
    was restarted after a crash). Callers just treat this like the
    ArcticService interface directly."""

    def __init__(self, timeout: float):
        self._timeout = timeout
        self._service = None

    def _ensure(self):
        if self._service is None:
            self._service = connect(timeout=self._timeout)
        return self._service

    def __getattr__(self, name):
        def call(*args, **kwargs):
            service = self._ensure()
            try:
                return getattr(service, name)(*args, **kwargs)
            except _RECONNECT_ERRORS:
                self._service = None
                service = self._ensure()
                return getattr(service, name)(*args, **kwargs)
        return call


_live_backend: _ResilientBackend | None = None


def get_live_backend(timeout: float = 15.0) -> _ResilientBackend:
    """Process-wide cached handle to the live Arctic singleton. Safe to call
    repeatedly/from multiple threads within one process — connecting is
    lazy (on first real use) and cached after that."""
    global _live_backend
    if _live_backend is None:
        _live_backend = _ResilientBackend(timeout)
    return _live_backend


def get_arctic_backend(local_arctic_url: str, timeout: float = 15.0):
    """The one place that decides "singleton server, or open the store
    directly": routes through the live Arctic server when running inside
    the orchestrated container (POWERTRADER_ARCTIC_SERVER=1, set by
    pt_controller.ProcessController.start_arctic_server() and inherited by
    every subprocess it launches from then on), otherwise opens
    `local_arctic_url` directly — e.g. standalone/test runs of pt_trainer.py
    or pt_web.py with no orchestrator around to have started the server,
    where there's no concurrent-process hazard to route around in the first
    place."""
    if os.environ.get("POWERTRADER_ARCTIC_SERVER"):
        return get_live_backend(timeout=timeout)
    from pt_arctic_server import ArcticService

    return ArcticService(local_arctic_url)
