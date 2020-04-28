import argparse
import os
from .tap import KindsOfSource


def parse_kind(args):
    kindParser = argparse.ArgumentParser(add_help=False)
    kindParser.add_argument('--kind', type=str, default=os.environ.get('PIPELINE', None),
                            choices=KindsOfSource(),
                            help='pulsar address (MEM is used mostly for testing')
    known, extras = kindParser.parse_known_args(args)
    return known.kind, extras
