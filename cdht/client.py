import socket
from abc import ABC, abstractmethod


class BaseClient(ABC):
    def __init__(self, family, stype):
        self._sock = socket.socket(family, stype)

    @property
    def socket(self):
        return self._sock

    @abstractmethod
    def send(self, buf):
        pass


class UDPClient(BaseClient):
    def __init__(self):
        super().__init__(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, addr, buf):
        with self.socket as sock:
            sock.sendto(buf, addr)


class TCPClient(BaseClient):
    def __init__(self, address):
        self._addr = address
        super().__init__(socket.AF_INET, socket.SOCK_STREAM)

    @property
    def address(self):
        return self._addr

    def send(self, buf):
        with self.socket as sock:
            sock.connect(self._addr)
            sock.sendall(buf)
