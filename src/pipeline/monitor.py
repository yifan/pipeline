import logging
import time

from prometheus_client import Counter, Enum, Info, start_http_server

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Monitor(object):
    info = Info('worker', 'worker information')
    err = Info('worker_error', 'worker error')
    counter = Counter(
        'worker_operation',
        'counter for worker operations',
        ['name', 'operation', 'topic']
    )
    state = Enum(
        'worker_state',
        'state of worker',
        ['name'],
        states=['starting', 'running', 'stopped'],
    )

    def __init__(self, worker, port=8000):
        self.worker = worker
        self.state.labels(name=worker.name).state('starting')
        self.port = port

    def expose(self):
        start_http_server(self.port)

    def record_worker_info(self):
        self.info.info({
            'name': self.worker.name,
            'version': self.worker.version,
            'description': self.worker.description,
            'in-topic': None if self.worker.has_no_input() else self.worker.source.topic,
            'out-topic': None if self.worker.has_no_output() else self.worker.destination.topic,
        })

    def record_start(self):
        self.info.info({
            'name': self.worker.name,
            'time': str(time.time()),
            'event': 'start',
        })
        self.state.labels(name=self.worker.name).state('running')

    def record_finish(self):
        self.info.info({
            'name': self.worker.name,
            'time': str(time.time()),
            'event': 'finish',
        })
        self.state.labels(name=self.worker.name).state('stopped')

    def record_error(self, msg):
        self.err.info({
            'name': self.worker.name,
            'time': str(time.time()),
            'event': 'error',
            'details': msg,
        })
        self.counter.labels(name=self.worker.name, operation='error', topic=None).inc()

    def record_write(self, topic=None):
        self.counter.labels(name=self.worker.name, operation='write', topic=topic).inc()

    def record_read(self, topic=None):
        self.counter.labels(name=self.worker.name, operation='read', topic=topic).inc()
