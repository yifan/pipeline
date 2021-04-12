import time
from typing import TYPE_CHECKING

from prometheus_client import Counter, Enum, Histogram, Info, start_http_server

if TYPE_CHECKING:
    from .worker import Worker


class Monitor(object):
    info = Info("worker", "worker information")
    err = Info("worker_error", "worker error")
    counter = Counter(
        "worker_operation",
        "counter for worker operations",
        ["name", "operation", "topic"],
    )
    state = Enum(
        "worker_state",
        "state of worker",
        ["name"],
        states=["starting", "running", "stopped"],
    )
    process_timer = Histogram("process_time_seconds", "Process time (seconds)")

    def __init__(self, worker: "Worker", port: int = 8000) -> None:
        self.worker = worker
        self.state.labels(name=worker.name).state("starting")
        self.port = port

    def expose(self) -> None:
        start_http_server(self.port)

    def record_worker_info(self) -> None:
        self.info.info(
            {
                "name": self.worker.name,
                "version": self.worker.version,
                "description": self.worker.description,
                "in-topic": self.worker.source.topic
                if self.worker.has_input()
                else None,
                "out-topic": self.worker.destination.topic
                if self.worker.has_output()
                else None,
            }
        )

    def record_start(self) -> None:
        self.info.info(
            {
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "start",
            }
        )
        self.state.labels(name=self.worker.name).state("running")

    def record_finish(self) -> None:
        self.info.info(
            {
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "finish",
            }
        )
        self.state.labels(name=self.worker.name).state("stopped")

    def record_error(self, msg: str) -> None:
        self.err.info(
            {
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "error",
                "details": msg,
            }
        )
        self.counter.labels(name=self.worker.name, operation="error", topic=None).inc()

    def record_write(self, topic: str = None) -> None:
        self.counter.labels(name=self.worker.name, operation="write", topic=topic).inc()

    def record_read(self, topic: str = None) -> None:
        self.counter.labels(name=self.worker.name, operation="read", topic=topic).inc()
