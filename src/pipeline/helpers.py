import argparse
import os
import sys
import time

from .cache import KindsOfCache
from .tap import KindsOfSource


def parse_kind(args):
    kindParser = argparse.ArgumentParser(add_help=False)
    kindParser.add_argument(
        "--kind",
        type=str,
        default=os.environ.get("PIPELINE", None),
        choices=KindsOfSource(),
        help="pipeline kind, can be {}".format(",".join(KindsOfSource())),
    )
    kindParser.add_argument(
        "--cacheKind",
        type=str,
        default=os.environ.get("CACHEKIND", None),
        choices=KindsOfCache(),
        help="cache kind, can be {}".format(",".join(KindsOfCache())),
    )
    known, extras = kindParser.parse_known_args(args)
    if known.kind is None:
        kindParser.print_help(sys.stderr)
    return known, extras


class Timer:
    def __init__(self):
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()
        self.totalTime = 0.0
        self.totalProcessTime = 0.0
        self.timeCount = 0
        self.processTimeCount = 0

    def elapsed_time(self):
        t = time.perf_counter() - self.startTime
        self.totalTime += t
        self.timeCount += 1
        return t

    def average_time(self):
        return self.totalTime / self.timeCount

    def process_time(self):
        t = time.process_time() - self.startProcessTime
        self.totalProcessTime += t
        self.processTimeCount += 1
        return t

    def average_process_time(self):
        return self.totalProcessTime / self.processTimeCount

    def start(self):
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()

    def log(self, logger):
        elapsedTime = self.elapsed_time()
        averageTime = self.average_time()
        logger.info(
            "Elapsed Time: %.2f, Average Time: %.2f", elapsedTime, averageTime
        )
        processTime = self.process_time()
        averageProcessTime = self.average_process_time()
        logger.info(
            "Process Time: %.2f, Average Process Time: %.2f",
            processTime,
            averageProcessTime,
        )
