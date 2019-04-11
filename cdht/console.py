"""Interactive console prompt."""
from cmd import Cmd
import logging

logger = logging.getLogger(__name__)


class CDHTPrompt(Cmd):
    prompt = '(cdht)'

    def __init__(self, peer):
        super().__init__()
        self._peer = peer

    def do_request(self, arg):
        """
        Request a file from peer.

        Example:
            (cdht) request 0159

        Argument must be:
            - a 4-digit number
            - cannot contain non-numeral character
        """
        if len(arg) != 4:
            print('Argument must be a 4-digit number')
            return

        try:
            _ = int(arg)
        except ValueError:
            print('Argument is not an integer')
            return

        self._peer.request_file(arg)

    def do_quit(self, arg):
        """Depart from network."""
        self._peer.depart_network()
        return True

    do_EOF = do_quit
