import argparse
import os
import sys

from .cache import KindsOfCache
from .tap import KindsOfSource


def parse_kind(args):
    kindParser = argparse.ArgumentParser(add_help=False)
    kindParser.add_argument('--kind', type=str, default=os.environ.get('PIPELINE', None),
                            choices=KindsOfSource(),
                            help='pipeline kind, can be {}'.format(','.join(KindsOfSource())))
    kindParser.add_argument('--cacheKind', type=str, default=os.environ.get('CACHEKIND', None),
                            choices=KindsOfCache(),
                            help='cache kind, can be {}'.format(','.join(KindsOfCache())))
    known, extras = kindParser.parse_known_args(args)
    if known.kind is None:
        kindParser.print_help(sys.stderr)
    return known, extras
