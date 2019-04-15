"""
Peer implementation.

Manages network topology.
"""

import logging
import socket
import socketserver
import threading
from time import sleep

from .client import TCPClient
from .config import (CDHT_HOST, CDHT_PING_INTERVAL_SEC, CDHT_PING_RETRIES,
                     CDHT_PING_TIMEOUT_SEC, CDHT_TCP_BASE_PORT,
                     CDHT_UDP_BASE_PORT)
from .protocol import Action, InvalidMessageError, Message, key_match_peer
from .server import CDHTMessageHandler, PingHandler

logger = logging.getLogger(__name__)


class Peer:
    """Peer that handles file transfer, ping, and manages successors."""

    def __init__(self,
                 peer_id,
                 succ_peer_id,
                 succ_peer_id_2,
                 mss,
                 drop_prob,
                 pred_peer_id=None,
                 pred_peer_id_2=None):
        self._id = peer_id
        self.pred_peer_id = pred_peer_id
        self.pred_peer_id_2 = pred_peer_id_2
        self._successors = [succ_peer_id, succ_peer_id_2]

        self._mss = mss
        self._drop_prob = drop_prob

        udp_addr = (CDHT_HOST, CDHT_UDP_BASE_PORT + self._id)

        self._ping_server = socketserver.ThreadingUDPServer(
            udp_addr, PingHandler)
        self._ping_server.peer = self
        self._ping_server_thread = threading.Thread(
            target=self._ping_server.serve_forever)

        self._ping_succ_thread = threading.Thread(target=self.ping_host,
                                                  args=(1, ),
                                                  daemon=True)
        self._ping_succ2_thread = threading.Thread(target=self.ping_host,
                                                   args=(2, ),
                                                   daemon=True)

        tcp_addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self._id)
        self._file_server = socketserver.ThreadingTCPServer(
            tcp_addr, CDHTMessageHandler)
        self._file_server.peer = self
        self._file_server_thread = threading.Thread(
            target=self._file_server.serve_forever)

    @property
    def id(self):
        return self._id

    @property
    def mss(self):
        return self._mss

    @property
    def drop_prob(self):
        return self._drop_prob

    @property
    def succ_peer_id(self):
        return self._successors[0]

    @succ_peer_id.setter
    def succ_peer_id(self, peer_id):
        self._successors[0] = peer_id

    @property
    def succ_peer_id_2(self):
        return self._successors[1]

    @succ_peer_id_2.setter
    def succ_peer_id_2(self, peer_id):
        self._successors[1] = peer_id

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
        self._ping_server.server_close()
        self._file_server.shutdown()
        self._file_server.server_close()
        self._ping_server_thread.join()
        self._file_server_thread.join()
        logger.debug('Servers all down.')

    def ping_host(self,
                  succ,
                  interval=CDHT_PING_INTERVAL_SEC,
                  timeout=CDHT_PING_TIMEOUT_SEC,
                  retries=CDHT_PING_RETRIES):
        """
        Send ping requests to a host using UDP.

        :param succ: peer to ping (1 or 2)
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            try_count = 0
            msg = Message(Action.PING_REQUEST)
            msg.sender = self.id
            msg.succ = succ
            while True:
                # smh I don't like this hack
                succ_peer_id = self.succ_peer_id if succ == 1 else self.succ_peer_id_2
                addr = (CDHT_HOST, CDHT_UDP_BASE_PORT + succ_peer_id)
                sock.sendto(msg.byte_string(), addr)
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
                        try:
                            # contact succ and find out who should connect to
                            self._handle_peer_churn(succ)
                        except socket.error:
                            logger.error(
                                f'No peer {succ_peer_id}. Not pinging until '
                                f'thread restarts.')
                            # exit thread
                            return
                    else:
                        # retry ping, skip sleep(interval)
                        continue
                except InvalidMessageError as e:
                    logger.exception('Network error', e)
                # sleep until next ping
                sleep(interval)

    def _handle_peer_churn(self, succ):
        if succ == 1:
            self.succ_peer_id = self.succ_peer_id_2
            self.succ_peer_id_2 = self.query_successor(self.succ_peer_id_2)
            logger.info(f'My first successor is now peer {self.succ_peer_id}.')
            logger.info(
                f'My second successor is now peer {self.succ_peer_id_2}')
        else:
            self.succ_peer_id_2 = self.query_successor(self.succ_peer_id)
            logger.info(f'My first successor is now peer {self.succ_peer_id}.')
            logger.info(
                f'My second successor is now peer {self.succ_peer_id_2}')

    def query_successor(self, peer):
        """
        Query peer for its successor.

        :param peer: peer to query for
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            msg = Message(Action.SUCC_QUERY)
            msg.sender = self.id
            sock.connect((CDHT_HOST, CDHT_TCP_BASE_PORT + peer))
            sock.sendall(msg.byte_string())
            response = Message.from_raw_buffer(sock.recv(1024))
            return response.succ

    def request_file(self, filename):
        """
        Search for a file, and send a request for the file to network.

        :param filename: name of the file
        """
        if not key_match_peer(self, filename):
            addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.succ_peer_id)
            request_client = TCPClient(addr)
            request = Message(Action.FILE_REQUEST)
            request.sender = self.id
            request.filename = filename
            request_client.send(request.byte_string())
            logger.info(
                f'File request message for {filename} has been sent to '
                f'my successor')
        else:
            raise ValueError('Assumption: requesting peer does not have the '
                             'that it is requesting.')

    def depart_network(self):
        """Gracefully depart from network."""

        def make_msg(succ, succ2):
            msg = Message(Action.PEER_DEPARTURE)
            msg.sender = self.id
            msg.succ = succ
            msg.succ2 = succ2
            return msg

        pred_addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.pred_peer_id)
        pred2_addr = (CDHT_HOST, CDHT_TCP_BASE_PORT + self.pred_peer_id_2)
        pred_client = TCPClient(pred_addr)
        pred_client2 = TCPClient(pred2_addr)
        pred_client.send(
            make_msg(self.succ_peer_id, self.succ_peer_id_2).byte_string())
        pred_client2.send(
            make_msg(self.pred_peer_id, self.succ_peer_id).byte_string())
        self.stop()
