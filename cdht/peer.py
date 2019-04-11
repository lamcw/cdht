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
from .protocol import MESSAGE_ENCODING, Action, InvalidMessageError, Message
from .server import FileRequestHandler, PingHandler

logger = logging.getLogger(__name__)


class Peer:
    def __init__(self,
                 peer_id,
                 succ_peer_id,
                 succ_peer_id_2,
                 pred_peer_id=None,
                 pred2_peer_id=None):
        self.id = peer_id
        self.pred_peer_id = pred_peer_id
        self.pred2_peer_id = pred2_peer_id
        self.succ_peer_id = succ_peer_id
        self.succ_peer_id_2 = succ_peer_id_2

        udp_addr = (CDHT_HOST, CDHT_UDP_BASE_PORT + self.id)
        udp_succ_peer_addr = (CDHT_HOST,
                              CDHT_UDP_BASE_PORT + self.succ_peer_id)
        udp_succ2_peer_addr = (CDHT_HOST,
                               CDHT_UDP_BASE_PORT + self.succ_peer_id_2)

        self._ping_server = socketserver.ThreadingUDPServer(
            udp_addr, PingHandler)
        self._ping_server.peer = self
        self._ping_server_thread = threading.Thread(
            target=self._ping_server.serve_forever, daemon=True)

        self._ping_succ_thread = threading.Thread(target=self.ping_host,
                                                  args=(udp_succ_peer_addr,
                                                        self.id))
        self._ping_succ2_thread = threading.Thread(target=self.ping_host,
                                                   args=(udp_succ2_peer_addr,
                                                         self.id))

        tcp_addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.id)
        self._file_request_server = socketserver.ThreadingTCPServer(
            tcp_addr, FileRequestHandler)
        self._file_request_server.peer = self
        self._file_request_server_thread = threading.Thread(
            target=self._file_request_server.serve_forever, daemon=True)

    def start(self):
        """Start the peer."""
        self._ping_server_thread.start()
        self._file_request_server_thread.start()
        self._ping_succ_thread.start()
        self._ping_succ2_thread.start()

        self._ping_succ_thread.join()
        logger.debug('succ ping thread exited')
        self._ping_succ2_thread.join()
        logger.debug('succ2 ping thread exited')

    def stop(self):
        """Stop this peer and kill all the servers."""
        self._ping_server.shutdown()
        self._file_request_server.shutdown()
        assert (not self._ping_server_thread.is_alive())
        assert (not self._file_request_server_thread.is_alive())

    def ping_host(self,
                  addr,
                  sender_id,
                  interval=CDHT_PING_INTERVAL_SEC,
                  timeout=CDHT_PING_TIMEOUT_SEC,
                  retries=CDHT_PING_RETRIES):
        """
        Send ping requests to a host using UDP.

        :param addr: address to send requests to
        :param sender_id: id of the sender peer
        """
        msg = Message(Action.PING_REQUEST)
        msg.sender = sender_id
        peer_id = addr[1] - CDHT_UDP_BASE_PORT

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            try_count = 0

            while True:
                sock.sendto(bytes(msg.format(), MESSAGE_ENCODING), addr)
                try:
                    data = sock.recv(1024)
                    recv_msg = Message.from_raw_buffer(data)
                    if recv_msg.sender != peer_id:
                        raise InvalidMessageError(
                            f'Wrong sender {recv_msg.sender}')
                    logger.info(
                        f'A ping response message was received from Peer {peer_id}'
                    )
                except socket.timeout:
                    logger.debug(f'Ping to peer {peer_id} timeout')
                    try_count += 1
                    if try_count >= retries:
                        logger.warning(f'Peer {peer_id} is no longer alive.')
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
        pass

    def depart_network(self):
        self.stop()
        logger.info(f'Peer {self.id} will depart from the network.')
