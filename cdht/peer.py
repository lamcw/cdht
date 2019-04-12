"""
Peer implementation.

Manages network topology.
"""

import logging
import socket
import socketserver
import threading
from time import sleep

from .config import (CDHT_HOST, CDHT_PING_INTERVAL_SEC, CDHT_PING_RETRIES,
                     CDHT_PING_TIMEOUT_SEC, CDHT_TCP_BASE_PORT,
                     CDHT_UDP_BASE_PORT)
from .protocol import (MESSAGE_ENCODING, Action, InvalidMessageError, Message,
                       key_match_peer)
from .server import FileMessageHandler, PingHandler
from .client import TCPClient

logger = logging.getLogger(__name__)


class Peer:
    def __init__(self,
                 peer_id,
                 succ_peer_id,
                 succ_peer_id_2,
                 pred_peer_id=None,
                 pred_peer_id_2=None):
        self.id = peer_id
        self.pred_peer_id = pred_peer_id
        self.pred_peer_id_2 = pred_peer_id_2
        self.succ_peer_id = succ_peer_id
        self.succ_peer_id_2 = succ_peer_id_2

        udp_addr = (CDHT_HOST, CDHT_UDP_BASE_PORT + self.id)

        self._ping_server = socketserver.ThreadingUDPServer(
            udp_addr, PingHandler)
        self._ping_server.peer = self
        self._ping_server_thread = threading.Thread(
            target=self._ping_server.serve_forever)

        self._ping_succ_thread = threading.Thread(target=self.ping_host,
                                                  args=(self.succ_peer_id, ),
                                                  daemon=True)
        self._ping_succ2_thread = threading.Thread(
            target=self.ping_host, args=(self.succ_peer_id_2, ), daemon=True)

        tcp_addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.id)
        self._file_server = socketserver.ThreadingTCPServer(
            tcp_addr, FileMessageHandler)
        self._file_server.peer = self
        self._file_server_thread = threading.Thread(
            target=self._file_server.serve_forever)

    def start(self):
        """Start the peer."""
        self._ping_server_thread.start()
        self._file_server_thread.start()
        self._ping_succ_thread.start()
        self._ping_succ2_thread.start()

    def stop(self):
        """Stop this peer and kill all the servers."""
        logger.debug('Killing all servers.')
        self._ping_server.shutdown()
        self._file_server.shutdown()
        self._ping_server_thread.join()
        self._file_server_thread.join()
        logger.debug('Servers all down.')
        assert (not self._ping_server_thread.is_alive())
        assert (not self._file_server_thread.is_alive())

    def ping_host(self,
                  succ_peer_id,
                  interval=CDHT_PING_INTERVAL_SEC,
                  timeout=CDHT_PING_TIMEOUT_SEC,
                  retries=CDHT_PING_RETRIES):
        """
        Send ping requests to a host using UDP.

        :param succ_peer_id: ID of peer to ping
        """
        msg = Message(Action.PING_REQUEST)
        msg.sender = self.id
        msg.succ = 1 if succ_peer_id == self.succ_peer_id else 2

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            try_count = 0
            addr = (CDHT_HOST, CDHT_UDP_BASE_PORT + succ_peer_id)

            while True:
                sock.sendto(bytes(msg.format(), MESSAGE_ENCODING), addr)
                try:
                    data = sock.recv(1024)
                    recv_msg = Message.from_raw_buffer(data)
                    if recv_msg.sender != succ_peer_id:
                        raise InvalidMessageError(
                            f'Wrong sender {recv_msg.sender}')
                    logger.info(
                        f'A ping response message was received from Peer '
                        f'{recv_msg.sender}')
                except socket.timeout:
                    logger.debug(f'Ping to peer {succ_peer_id} timeout')
                    try_count += 1
                    if try_count >= retries:
                        logger.warning(
                            f'Peer {succ_peer_id} is no longer alive.')
                        # TODO contact succ and find out who should connect to
                        return
                    else:
                        # retry ping, skip sleep(interval)
                        continue
                except InvalidMessageError as e:
                    logger.exception('Network error', e)
                # sleep until next ping
                sleep(interval)

    def request_file(self, filename):
        def forward_callback():
            addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.succ_peer_id)
            request_client = TCPClient(addr)
            request = Message(Action.FILE_REQUEST)
            request.sender = self.id
            request.filename = filename
            request_client.send(request.byte_string())
            logger.info(
                f'File request message for {filename} has been sent to '
                f'my successor')

        key_match_peer(self, filename, forward_callback, lambda: None)

    def depart_network(self):
        logger.info(f'Peer {self.id} will depart from the network.')
        self.stop()
