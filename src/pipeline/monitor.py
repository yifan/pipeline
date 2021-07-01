import time
from typing import TYPE_CHECKING

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Enum,
    Histogram,
    Info,
    start_http_server,
)

if TYPE_CHECKING:
    from .worker import Worker


class Monitor(object):
    def __init__(self, port: int = 8000) -> None:
        self.registry = CollectorRegistry()
        self.port = port
        self.metrics = {}

    def use_counter(self, name, description, labels):
        if name not in self.metrics:
            self.metrics[name] = Counter(
                name,
                description,
                labels,
                registry=self.registry,
            )

        return self.metrics[name]

    def counter(self, name, labels={}):
        self.metrics[name].labels(**labels).inc()

    def use_info(self, name, description):
        if name not in self.metrics:
            self.metrics[name] = Info(
                name,
                description,
                registry=self.registry,
            )

        return self.metrics[name]

    def info(self, name, labels={}):
        self.metrics[name].info(labels)

    def use_state(self, name, description, labels, states):
        if name not in self.metrics:
            self.metrics[name] = Enum(
                name,
                description,
                labelnames=labels,
                states=states,
                registry=self.registry,
            )
        return self.metrics[name]

    def state(self, name, labels, state=None):
        self.metrics[name].labels(**labels).state(state)

    def use_histogram(self, name, description):
        if name not in self.metrics:
            self.metrics[name] = Histogram(
                name,
                description,
                registry=self.registry,
            )

        return self.metrics[name]

    def histogram(self, name):
        return self.metrics[name]

    def expose(self) -> None:
        start_http_server(self.port)


class WorkerMonitor(Monitor):
    def __init__(self, worker: "Worker", port: int = 8000) -> None:
        super().__init__(port)
        self.worker = worker

        self.setup()

    def setup(self):
        self.worker_info = self.use_info("worker", "worker information")
        self.worker_error = self.use_info("worker_error", "worker error")
        self.worker_state = self.use_state(
            "worker_state",
            "worker state, eg: starting, running and stopped",
            labels=["name"],
            states=["starting", "running", "stopped"],
        ).labels(name=self.worker.name)
        self.worker_state.state("starting")
        self.worker_operation = self.use_counter(
            "worker_operation",
            "counter for worker operations",
            labels=["name", "operation", "topic"],
        )
        self.process_timer = self.use_histogram(
            "process_time_seconds",
            "Process time (seconds)",
        )

    def record_worker_info(self) -> None:
        self.info(
            "worker",
            labels={
                "name": self.worker.name,
                "version": self.worker.version,
                "description": self.worker.description,
                "in-topic": self.worker.source.topic
                if self.worker.has_input()
                else None,
                "out-topic": self.worker.destination.topic
                if self.worker.has_output()
                else None,
            },
        )

    def record_start(self) -> None:
        self.info(
            "worker",
            labels={
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "start",
            },
        )
        self.state(
            "worker_state",
            labels=dict(name=self.worker.name),
            state="running",
        )

    def record_finish(self) -> None:
        self.info(
            "worker",
            labels={
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "finish",
            },
        )
        self.state(
            "worker_state",
            labels=dict(name=self.worker.name),
            state="stopped",
        )

    def record_error(self, msg: str) -> None:
        self.info(
            "worker",
            labels={
                "name": self.worker.name,
                "time": str(time.time()),
                "event": "error",
                "details": msg,
            },
        )
        self.counter(
            "worker_operation",
            labels=dict(name=self.worker.name, operation="error", topic=None),
        )

    def record_write(self, topic: str = None) -> None:
        self.counter(
            "worker_operation",
            labels=dict(name=self.worker.name, operation="write", topic=topic),
        )

    def record_read(self, topic: str = None) -> None:
        self.counter(
            "worker_operation",
            labels=dict(name=self.worker.name, operation="read", topic=topic),
        )
