"""Server implementation used in CDHT."""
import logging
import socketserver
from copy import deepcopy

from .client import TCPClient
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


class PingHandler(MessageDeserializerMixin,
                  socketserver.DatagramRequestHandler):
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

        if self.message.succ == 1:
            self.server.peer.pred_peer_id = self.message.sender
        else:
            self.server.peer.pred_peer_id_2 = self.message.sender
        logger.debug(f'pred={self.server.peer.pred_peer_id}')
        logger.debug(f'pred={self.server.peer.pred_peer_id_2}')

        response = Message(Action.PING_RESPONSE)
        response.sender = self.server.server_address[1] - CDHT_UDP_BASE_PORT
        self.wfile.write(bytes(response.format(), MESSAGE_ENCODING))


class FileMessageHandler(MessageDeserializerMixin,
                         socketserver.StreamRequestHandler):
    def handle(self):
        """Handle a file request ,transfer and ack."""
        action = self.message.action

        if (action == Action.FILE_REQUEST
                or action == Action.FILE_REQUEST_FORWARD):
            key_match_peer(self.server.peer, self.message.filename,
                           self.forward, self.process_request)
        elif action == Action.FILE_REQUEST_ACK:
            logger.info(f'Received a response message from peer '
                        f'{self.message.sender}, which  has the file '
                        f'{self.message.filename}')
            logger.info('We now start receiving the file...')
        else:
            raise InvalidMessageError(
                f'Invalid message over TCP: {self.message}')

    def forward(self):
        """File is not on this host, forward request to immediate successor."""
        logger.info(f'File {self.message.filename} is not stored here.')
        addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.server.peer.succ_peer_id)
        forward_client = TCPClient(addr)
        forwarded_msg = deepcopy(self.message)
        forwarded_msg.action = Action.FILE_REQUEST_FORWARD
        forward_client.send(forwarded_msg.byte_string())
        logger.info('File request message has been forwarded to my successor.')

    def process_request(self):
        """File is on this host, ack and transmit."""
        logger.info(f'File {self.message.filename} is here.')
        addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.message.sender)
        response_client = TCPClient(addr)
        response = Message(Action.FILE_REQUEST_ACK)
        response.sender = self.server.server_address[1] - CDHT_TCP_BASE_PORT
        response.filename = self.message.filename
        response_client.send(response.byte_string())
        logger.info(f'A response message, destined for peer '
                    f'{self.message.sender}, has been sent')
        # TODO transfer file
