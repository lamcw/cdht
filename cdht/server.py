"""Server implementation used in CDHT."""
import logging
import socket
import socketserver
import time
from base64 import b64decode, b64encode
from collections import deque
from copy import deepcopy
from threading import Thread

from .client import TCPClient
from .config import (CDHT_HOST, CDHT_TCP_BASE_PORT, CDHT_TRANSFER_BASE_PORT,
                     CDHT_TRANSFER_TIMEOUT_SEC, CDHT_UDP_BASE_PORT)
from .protocol import (MESSAGE_ENCODING, Action, InvalidMessageError, Message,
                       key_match_peer)

logger = logging.getLogger(__name__)


class LogMixin:
    log_name = 'log'

    def setup(self):
        super().setup()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.log_name, mode='w', delay=True)
        fh.setLevel(logging.INFO)
        self.logger.addHandler(fh)
        self.logger.propagate = False


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
        """Handle a file request, transfer and ack."""
        action = self.message.action

        if (action == Action.FILE_REQUEST
                or action == Action.FILE_REQUEST_FORWARD):
            if key_match_peer(self.server.peer, self.message.filename):
                self.process_request()
            else:
                self.forward()
        elif action == Action.FILE_REQUEST_ACK:
            logger.info(f'Received a response message from peer '
                        f'{self.message.sender}, which  has the file '
                        f'{self.message.filename}')
            logger.info('We now start receiving the file...')
            addr = (CDHT_HOST, CDHT_TRANSFER_BASE_PORT + self.server.peer.id)
            with socketserver.UDPServer(addr, FileReceiveHandler) as server:
                server.peer = self.server.peer
                server.serve_forever()
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
        # transfer file
        addr = (CDHT_HOST, CDHT_TRANSFER_BASE_PORT + self.message.sender)
        server_addr = (CDHT_HOST,
                       CDHT_TRANSFER_BASE_PORT + self.server.peer.id)
        # wait for client to set up a server to receive packets
        time.sleep(0.5)
        send_file(addr, server_addr, self.message.filename,
                  self.server.peer.mss, self.server.peer.drop_prob)


class FileSendHandler(MessageDeserializerMixin, LogMixin,
                      socketserver.DatagramRequestHandler):
    timeout = CDHT_TRANSFER_TIMEOUT_SEC
    log_name = 'responding_log.txt'

    def setup(self):
        super().setup()

        if self.timeout is not None:
            self.socket.settimeout(self.timeout)

    def handle(self):
        action = self.message.action
        if action == Action.FILE_TRANSFER_ACK:
            if self.message.ack > self.server.in_transit_packet:
                raise InvalidMessageError(f'ACKing future packets: '
                                          'ack={self.message.ack}')
            # send next packet
            self.server.in_transit_packet = self.server.packet_queue.pop()
            self.wfile.write(self.server.in_transit_packet.byte_string())
            pkt = self.server.in_transit_packet
            self.logger.info(
                f'snd\ttime\t{pkt.seq}\t{len(pkt.raw)}\t{pkt.ack}')
        else:
            raise InvalidMessageError(
                f'Invalid message over UDP: {self.message}')


class FileReceiveHandler(MessageDeserializerMixin, LogMixin,
                         socketserver.DatagramRequestHandler):
    log_name = 'requesting_log.txt'

    def handle(self):
        """Handle received file chunks."""
        if self.message.action == Action.FILE_TRANSFER:
            msg = self.message
            chunk = b64decode(msg.raw)
            self.logger.info(
                f'rcv\t\ttime\t\t{msg.seq}\t\t{len(chunk)}\t\t{msg.ack}')
            self.ack(len(chunk) + 1, msg.mss)

            if hasattr(msg, 'last') and msg.last:
                logger.info('The file is received.')
                Thread(target=self.kill_server).start()
        else:
            raise InvalidMessageError(
                'Message type is not Action.FILE_TRANSFER')

    def ack(self, ack_no, mss):
        """
        Send ACK message to packet sender.

        :param ack_no: corresponding ack number
        :param mss: mss used (for logging)
        """
        ack = Message(Action.FILE_REQUEST_ACK)
        ack.sender = self.server.peer.id
        ack.seq = 0
        ack.ack = ack_no
        addr = (CDHT_HOST, CDHT_TRANSFER_BASE_PORT + self.message.sender)

        self.logger.info(f'snd\t\ttime\t\t{ack.seq}\t\t{mss}\t\t{ack_no}')

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(ack.byte_string(), addr)

    def kill_server(self):
        """
        Kill server from within handler.

        Should only be called in a thread.
        """
        self.server.shutdown()
        self.server.server_close()


def send_file(addr, server_addr, file, mss, drop_prob):
    """Send file to host."""
    # TODO remove
    file = file + ".pdf"
    with open(file, 'rb') as f:
        buffer = f.read()
        chunks = [buffer[0 + i:i + mss] for i in range(0, len(buffer), mss)]

    def packet_generator():
        last = len(chunks) - 1
        for n, chunk in enumerate(chunks):
            packet = Message(Action.FILE_TRANSFER)
            packet.sender = server_addr[1] - CDHT_TRANSFER_BASE_PORT
            packet.seq = n * mss + 1
            packet.mss = mss
            # To serialize message to json, convert to base64 bytes, then
            # decode according to default encoding
            b64_bytes = b64encode(chunk)
            utf8_string = b64_bytes.decode(MESSAGE_ENCODING)
            packet.raw = utf8_string
            packet.ack = 0
            if n == last:
                packet.last = True
            yield packet

    logger.info('We now start sending the file...')
    # packet_queue = deque(packet_generator(chunks))
    # pkt = packet_queue.pop()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        for pkt in packet_generator():
            sock.sendto(pkt.byte_string(), addr)
    logger.info('The file is sent')

    # with socketserver.UDPServer(server_addr, FileSendHandler) as server:
    #     server.packet_queue = packet_queue
    #     server.serve_forever()
    # TODO send first packet
