import argparse
import os

from .data import KindsOfDataReader
from .tap import KindsOfSource


def parse_kind(args):
    kindParser = argparse.ArgumentParser(add_help=False)
    kindParser.add_argument('--kind', type=str, default=os.environ.get('PIPELINE', None),
                            choices=KindsOfSource(),
                            help='pipeline kind, can be {}'.format(','.join(KindsOfSource())))
    kindParser.add_argument('--dataKind', type=str, default=os.environ.get('DATAKIND', None),
                            choices=KindsOfDataReader(),
                            help='data kind, can be {}'.format(','.join(KindsOfDataReader())))
    known, extras = kindParser.parse_known_args(args)
    return known, extras
