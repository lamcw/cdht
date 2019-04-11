import socket


class Client:
    def __init__(self, family, stype, address):
        self._sock = socket.socket(family, stype)


class UDPClient:
    def __init__(self, address):
        super().__init__(socket.AF_INET, socket.SOCK_DGRAM, address)


class TCPClient:
    def __init__(self, address):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM, address)
