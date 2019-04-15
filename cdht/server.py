"""Server implementation used in CDHT."""
import logging
import random
import socketserver
import time
from base64 import b64decode, b64encode
from copy import deepcopy
from threading import Thread, Timer

from .client import TCPClient, UDPClient
from .config import (CDHT_HOST, CDHT_TCP_BASE_PORT, CDHT_TRANSFER_BASE_PORT,
                     CDHT_TRANSFER_TIMEOUT_SEC, CDHT_UDP_BASE_PORT)
from .protocol import (MESSAGE_ENCODING, Action, InvalidMessageError, Message,
                       key_match_peer)

logger = logging.getLogger(__name__)


class LogMixin:
    log_name = 'log'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.log_name, mode='a', delay=True)
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


class CDHTMessageHandler(MessageDeserializerMixin,
                         socketserver.StreamRequestHandler):
    receive_filename = 'received_file.pdf'

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
            with FileReceiveServer(addr,
                                   FileReceiveHandler,
                                   recv_file=self.receive_filename) as server:
                server.peer = self.server.peer
                server.serve_forever()
        elif action == Action.PEER_DEPARTURE:
            self.peer_depart()
        elif action == Action.SUCC_QUERY:
            response = Message(Action.SUCC_QUERY_RESPONSE)
            response.sender = self.server.peer.id
            response.succ = self.server.peer.succ_peer_id
            self.wfile.write(response.byte_string())
        else:
            raise InvalidMessageError(
                f'Invalid message over TCP: {self.message}')

    def peer_depart(self):
        if (self.message.sender == self.server.peer.succ_peer_id
                or self.message.sender == self.server.peer.succ_peer_id_2):
            logger.info(
                f'Peer {self.message.sender} will depart from network.')
            self.server.peer.succ_peer_id = self.message.succ
            self.server.peer.succ_peer_id_2 = self.message.succ2
            logger.info(
                f'My first successor is now peer {self.server.peer.succ_peer_id}'
            )
            logger.info(
                f'My second successor is now peer {self.server.peer.succ_peer_id_2}'
            )

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
        time.sleep(0.05)
        send_file(addr, server_addr, self.message.filename,
                  self.server.peer.mss, self.server.peer.drop_prob)


class FileSendHandler(MessageDeserializerMixin,
                      socketserver.DatagramRequestHandler):
    def handle(self):
        msg = self.message
        action = msg.action
        if action == Action.FILE_TRANSFER_ACK:
            if msg.ack == self.server.in_transit_packet_ack:
                pkt = self.server.in_transit_packet
                self.server.logger.info(
                    f'rcv\t\ttime\t\t{msg.seq}\t\t{msg.rcv}\t\t{msg.ack}')
                self.server.queue_next_packet()
                if not self.server.is_alive:
                    # job queue is empty
                    Thread(target=self.server.shutdown).start()
                    return
                self.server.restart_timer()
                if random.uniform(0, 1) < self.server.drop_prob:
                    # drop packet
                    self.server.logger.info(
                        f'drop\t\ttime\t\t{pkt.seq}\t\t{len(pkt.raw)}\t\t{pkt.ack}'
                    )
                    return
                # send next packet
                self.server.send_packet()
                pkt = self.server.in_transit_packet
                self.server.logger.info(
                    f'snd\t\ttime\t\t{pkt.seq}\t\t{len(pkt.raw)}\t\t{pkt.ack}')
            elif msg.ack > self.server.in_transit_packet_ack:
                raise InvalidMessageError(f'ACKing future packets: '
                                          'ack={self.message.ack}')
            else:
                logger.debug('Ignored ACK')
        else:
            raise InvalidMessageError(
                f'Invalid message over UDP: {self.message}')


class FileReceiveHandler(MessageDeserializerMixin,
                         socketserver.DatagramRequestHandler):
    def handle(self):
        """Handle received file chunks."""
        if self.message.action == Action.FILE_TRANSFER:
            msg = self.message
            self.server.file_buffer_append(msg.raw)
            self.server.logger.info(
                f'rcv\t\ttime\t\t{msg.seq}\t\t{len(msg.raw)}\t\t{msg.ack}')
            self.ack(msg.seq + len(msg.raw), len(msg.raw))

            if hasattr(msg, 'last') and msg.last:
                # handling last packet
                logger.info('The file is received.')
                self.server.file_buffer_write()
                self.server.recv_file.close()
                Thread(target=self.kill_server).start()
        else:
            raise InvalidMessageError(
                'Message type is not Action.FILE_TRANSFER')

    def ack(self, ack_no, pkt_size):
        """
        Send ACK message to packet sender.

        :param ack_no: corresponding ack number
        :param pkt_size: number of bytes received
        """
        ack = Message(Action.FILE_TRANSFER_ACK)
        ack.sender = self.server.peer.id
        ack.seq = 0
        ack.ack = ack_no
        ack.rcv = pkt_size

        self.server.logger.info(
            f'snd\t\ttime\t\t{ack.seq}\t\t{ack.rcv}\t\t{ack.ack}')
        ack_client = UDPClient()
        ack_client.send(
            ack.byte_string(),
            (CDHT_HOST, CDHT_TRANSFER_BASE_PORT + self.message.sender))

    def kill_server(self):
        """
        Kill server from within handler.

        Should only be called in a thread.
        """
        self.server.shutdown()
        self.server.server_close()


class FileReceiveServer(LogMixin, socketserver.UDPServer):
    log_name = 'requesting_log.txt'

    def __init__(self, *args, recv_file=None, **kwargs):
        super().__init__(*args, **kwargs)
        if recv_file:
            self.recv_file = open(recv_file, 'w+b')
        self._buffer = ''

    def file_buffer_append(self, buf):
        self._buffer += buf

    def file_buffer_write(self):
        self.recv_file.write(b64decode(self._buffer))


class FileSendServer(LogMixin, socketserver.UDPServer):
    log_name = 'responding_log.txt'
    timeout = CDHT_TRANSFER_TIMEOUT_SEC

    def __init__(self,
                 server_address,
                 RequestHandlerClass,
                 dest_addr,
                 packets,
                 drop_prob,
                 bind_and_activate=True,
                 **kwargs):
        super().__init__(server_address, RequestHandlerClass,
                         bind_and_activate, **kwargs)
        self.drop_prob = drop_prob
        self._packets = packets
        self._in_transit_packet_index = 0
        self._dest_addr = dest_addr
        self._client = UDPClient()

        # send the first packet
        self.send_packet()
        pkt = self.in_transit_packet
        self.logger.info(
            f'snd\t\ttime\t\t{pkt.seq}\t\t{len(pkt.raw)}\t\t{pkt.ack}')
        self._timeout_thread = Timer(CDHT_TRANSFER_TIMEOUT_SEC,
                                     self._resend_packet)
        self._timeout_thread.start()

    @property
    def in_transit_packet(self):
        return self._packets[self._in_transit_packet_index]

    @property
    def in_transit_packet_ack(self):
        return self.in_transit_packet.seq + len(self.in_transit_packet.raw)

    @property
    def is_alive(self):
        return self._in_transit_packet_index < len(self._packets)

    def send_packet(self):
        if self.is_alive:
            try:
                pkt = self.in_transit_packet
                self._client.send(pkt.byte_string(), self._dest_addr)
            except IndexError:
                # Queue empty
                self._timeout_thread.cancel()

    def _resend_packet(self):
        pkt = self.in_transit_packet
        self.logger.info(
            f'RTX\t\ttime\t\t{pkt.seq}\t\t{len(pkt.raw)}\t\t{pkt.ack}')
        self.restart_timer()
        self.send_packet()

    def queue_next_packet(self):
        self._in_transit_packet_index += 1

    def restart_timer(self):
        self._timeout_thread.cancel()
        self._timeout_thread = Timer(CDHT_TRANSFER_TIMEOUT_SEC,
                                     self._resend_packet)
        self._timeout_thread.start()

    def __exit__(self, *args):
        super().__exit__(*args)
        # cleanup threads
        self._timeout_thread.cancel()


def send_file(addr, server_addr, file, mss, drop_prob):
    """Send file to host."""
    # TODO remove
    file = file + ".pdf"
    with open(file, 'rb') as f:
        buffer = f.read()
        # To serialize message to json, convert to base64 bytes, then
        # decode according to default encoding because json cannot handle bytes
        b64_buffer = b64encode(buffer)
        utf8_buffer = b64_buffer.decode(MESSAGE_ENCODING)
        chunks = [
            utf8_buffer[0 + i:i + mss] for i in range(0, len(utf8_buffer), mss)
        ]

    def packet_generator():
        last = len(chunks) - 1
        for n, chunk in enumerate(chunks):
            packet = Message(Action.FILE_TRANSFER)
            packet.sender = server_addr[1] - CDHT_TRANSFER_BASE_PORT
            packet.seq = n * mss + 1
            packet.mss = mss
            packet.raw = chunk
            packet.ack = 0
            if n == last:
                packet.last = True
            yield packet

    logger.info('We now start sending the file...')
    with FileSendServer(server_addr, FileSendHandler, addr,
                        list(packet_generator()), drop_prob) as server:
        server.serve_forever()
    logger.info('The file is sent')
