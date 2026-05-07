"""Prometheus scrape server lifecycle.

Wraps `prometheus_client.start_http_server` so the gateway boots /
shuts down the metrics endpoint as a peer of the WS / gRPC / REST
servers in `__main__.py`.

The HTTP server runs on its own daemon thread (that's how
`prometheus_client.start_http_server` works); it does not block the
asyncio event loop. `start()` therefore returns immediately and the
TaskGroup the gateway runs against doesn't need to host a metrics
coroutine — it just calls `stop()` on shutdown to close the listener
cleanly.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.gc_collector import GC_COLLECTOR
from prometheus_client.platform_collector import PLATFORM_COLLECTOR
from prometheus_client.process_collector import PROCESS_COLLECTOR

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    import threading
    from http.server import HTTPServer

log = get_logger(__name__)


class MetricsServer:
    """Owns the lifecycle of the Prometheus scrape endpoint.

    Construct, then call `start()` once at boot. Call `stop()` on
    shutdown. Both are idempotent — repeated `start()` is a no-op,
    repeated `stop()` is a no-op.

    The class deliberately does NOT integrate with the path setting
    (`metrics_path`); `prometheus_client.start_http_server` always
    serves on `/`, plus a redirect from `/metrics`. We accept the
    library's behaviour here — most scrapers hit the root anyway, and
    the path setting is for the rare reverse-proxy use case which
    handles the path rewrite externally.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        include_python_collectors: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._include_python_collectors = include_python_collectors
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    async def start(self) -> None:
        if self._server is not None:
            return  # idempotent

        # Default collectors register themselves on `prometheus_client`
        # import; opting out is a one-time unregister call. Doing this
        # before the listener binds means `/metrics` never exposes the
        # `python_*` / `process_*` series at all.
        if not self._include_python_collectors:
            for collector in (GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR):
                # Already removed if test isolation pruned them earlier
                # in the same process; suppress KeyError so a re-start
                # is idempotent.
                with contextlib.suppress(KeyError):
                    REGISTRY.unregister(collector)

        # `start_http_server` returns `(HTTPServer, Thread)` since
        # prometheus-client 0.20. The server's request handling runs
        # on the returned daemon thread; the asyncio loop is not
        # involved.
        self._server, self._thread = start_http_server(
            port=self._port,
            addr=self._host,
        )
        log.info(
            "metrics_server.started",
            host=self._host,
            port=self._port,
            include_python_collectors=self._include_python_collectors,
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        # `shutdown()` returns when the request loop exits; close the
        # socket and join the thread to avoid a zombie listener if the
        # process restarts in-place (rare, but matters for tests that
        # re-bind the same port back-to-back).
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._server = None
        self._thread = None
        log.info("metrics_server.stopped")
