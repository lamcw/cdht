"""Server implementation used in CDHT."""
import logging
import socket
import socketserver
from copy import deepcopy

from .config import CDHT_HOST, CDHT_TCP_BASE_PORT, CDHT_UDP_BASE_PORT
from .protocol import (MESSAGE_ENCODING, Action, InvalidMessageError, Message,
                       key_match_peer)

logger = logging.getLogger(__name__)


class MessageDeserializerMixin:
    """Deserialize message before passing to handle().

    This mixin is meant to be used with socketserver.DatagramRequestHandler
    and/or socketserver.StreamRequestHandler.
    """

    def setup(self):
        """Initialize self.message with `Message`."""
        super().setup()
        nbytes = int(self.rfile.readline().strip())
        json_str = self.rfile.read(nbytes)
        self.message = Message.from_json(json_str)


class PingHandler(socketserver.DatagramRequestHandler,
                  MessageDeserializerMixin):
    """
    Handle ping requests.

    Requests should have action == Action.PING_REQUEST.
    """

    def handle(self):
        if self.message.action != Action.PING_REQUEST:
            raise InvalidMessageError('Not a ping request.')

        logger.info(
            f'A ping request message was received from Peer {self.message.sender}'
        )

        response = Message(Action.PING_RESPONSE)
        response.sender = self.server.server_address[1] - CDHT_UDP_BASE_PORT
        self.wfile.write(bytes(response.format(), MESSAGE_ENCODING))


class FileRequestHandler(socketserver.StreamRequestHandler,
                         MessageDeserializerMixin):
    def handle(self):
        if (self.message.action != Action.FILE_REQUEST
                or self.message.action != Action.FILE_REQUEST_FORWARD):
            raise InvalidMessageError('Not a file request.')

        peer_id = self.server.server_address[1] - CDHT_TCP_BASE_PORT
        key_match_peer(peer_id, self.message.filename, self.forward,
                       self.process_request)

    def forward(self):
        """File is not on this host, forward request to immediate successor."""
        logger.info(f'File {self.message.filename} is not stored here.')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((CDHT_HOST,
                          CDHT_TCP_BASE_PORT + self.server.peer.succ_peer_id))
            forwarded_msg = deepcopy(self.message)
            forwarded_msg.action = Action.FILE_REQUEST_FORWARD
            sock.sendall(bytes(forwarded_msg.format(), MESSAGE_ENCODING))
            logger.info(
                'File request message has been forwarded to my successor.')

    def process_request(self):
        """File is on this host, ack and transmit."""
        logger.info(f'File {self.message.filename} is here.')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((CDHT_HOST, CDHT_TCP_BASE_PORT + self.message.sender))
            response = Message(Action.FILE_REQUEST_ACK)
            response.sender = self.server.server_address[1] - CDHT_TCP_BASE_PORT
            sock.sendall(bytes(response.format(), MESSAGE_ENCODING))
        logger.info(f'A response message, destined for peer'
                    '{self.message.sender}, has been sent')
