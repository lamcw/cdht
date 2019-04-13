"""CLI module."""
import argparse
import logging
import threading
from decimal import Decimal

from .config import CDHT_PEER_ID_RANGE
from .console import CDHTPrompt
from .peer import Peer

logger = logging.getLogger(__name__)


def peer_id(value):
    """Check if peer identity is between 0 and 255."""
    value = int(value)
    if value not in CDHT_PEER_ID_RANGE:
        raise argparse.ArgumentTypeError("Must be between 0 and 255")
    return value


def log_level(value):
    """Check if log level string is valid."""
    try:
        level = getattr(logging, value.upper())
    except AttributeError:
        raise argparse.ArgumentTypeError(
            f"Invalid log level: {value}. See pydoc logging.")
    return level


def run():
    """CDHT cli entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('peer_id', type=peer_id, help='Local peer ID')
    parser.add_argument('succ_peer_id',
                        type=peer_id,
                        help='ID of successive peer')
    parser.add_argument('succ_peer_id_2',
                        type=peer_id,
                        help='ID of second successive peer')
    parser.add_argument('mss',
                        type=int,
                        help='Maximum segment size (in bytes)')
    parser.add_argument('drop_probability',
                        type=Decimal,
                        help='Drop packets according to this probability')
    parser.add_argument(
        '-l',
        '--log',
        type=log_level,
        metavar='LOG_LEVEL',
        default=logging.INFO,
        help='Set log level. LOG_LEVEL can be one of: NOTSET, DEBUG, INFO, '
        'WARNING, WARN, ERROR, CRITICAL, FATAL.')
    args = parser.parse_args()

    logging.basicConfig(level=args.log)

    peer = Peer(args.peer_id, args.succ_peer_id, args.succ_peer_id_2, args.mss,
                args.drop_probability)
    prompt = CDHTPrompt(peer)
    prompt_thread = threading.Thread(target=prompt.cmdloop)

    prompt_thread.start()
    peer.start()
