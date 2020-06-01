import argparse
import os

from types import SimpleNamespace

from .tap import KindsOfSource


def parse_kind(args):
    kindParser = argparse.ArgumentParser(add_help=False)
    kindParser.add_argument('--kind', type=str, default=os.environ.get('PIPELINE', None),
                            choices=KindsOfSource(),
                            help='pulsar address (MEM is used mostly for testing')
    known, extras = kindParser.parse_known_args(args)
    return known.kind, extras


def parse_connection_string(connectionString, no_port=False, no_username=False):
    """ Parse connection string in user:password@host:port format

    >>> parse_connection_string("username:password@host:port")
    namespace(host='host', password='password', port='port', username='username')
    >>> parse_connection_string("username@host")
    namespace(host='host', password=None, port=None, username='username')
    >>> parse_connection_string("username:password@host:port", no_port=True)
    namespace(host='host:port', password='password', port=None, username='username')
    >>> parse_connection_string("password@host:port", no_username=True)
    namespace(host='host', password='password', port='port', username=None)
    """
    *userNameAndPasswordOrEmpty, remaining = connectionString.split('@')
    password = None
    if userNameAndPasswordOrEmpty:
        if no_username:
            username, password = None, userNameAndPasswordOrEmpty[0]
        else:
            username, *passwordOrEmpty = userNameAndPasswordOrEmpty[0].split(':')
            if passwordOrEmpty:
                password = passwordOrEmpty[0]
    else:
        username = None

    port = None
    if no_port:
        host = remaining
    else:
        host, *portOrEmpty = remaining.split(':')
        if portOrEmpty:
            port = portOrEmpty[0]

    return SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
    )
