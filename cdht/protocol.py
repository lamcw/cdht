"""
Application layer protocol design.

0. Departing the network (gracefully)
======================
10 send to 8:
{
    'sender': 10,
    'action': PEER_DEPARTURE,
    'succ': 12,
    'succ2': 15
}

10 send to 5:
{
    'sender': 10,
    'action': PEER_DEPARTURE,
    'succ': 8,
    'succ2': 12
}

1. Ping (UDP)
=======
Send:
{
    'action': PING_REQUEST,
    'sender': 1
    'succ': 1
}

Receive:
{
    'action:' PING_RESPONSE,
    'sender': 2
}

2. Transfer
===========
Send:
{
    'action': FILE_REQUEST,
    'sender': 1,
    'filename': '0159'
}

{
    'action': FILE_REQUEST_FORWARD,
    'sender': 1,
    'filename': '0159'
}

Receive:
{
    'sender': 4,
    'action': FILE_REQUEST_ACK
    'filename': '0159'
}
{
    'sender': 4,
    'action': FILE_TRANSFER,
    'seq': 0,
    'ack': 0,
    'mss': 400,
    'raw': ...
}
{
    'sender': 8,
    'action': FILE_TRANSFER_ACK,
    'seq': 0
    'ack': 401
}
{
    'sender': 4,
    'action': FILE_TRANSFER,
    'seq': 801,
    'last': true
    'raw': ...
}

3. Departing the network
========================
"""

import json
import logging
from enum import IntEnum, unique

logger = logging.getLogger(__name__)

MESSAGE_ENCODING = 'utf-8'


def cdht_hash(n, factor=256):
    """Hash filename according to protocol."""
    return int(n) % factor


def key_match_peer(peer, key):
    """
    Determine if key matches a peer.

    :param peer: peer
    :param key: filename
    """
    hashed_value = cdht_hash(key)
    if hashed_value > peer.id:
        if peer.pred_peer_id > peer.id:
            return True
        else:
            return False
    return True


class InvalidMessageError(ValueError):
    """Raised when message is invalid."""

    pass


class JsonSerializable:
    """Mixin class that allows a class to be serialized."""

    def json(self, *args, **kwargs):
        """Get JSON string from cls.__dict__."""
        return json.dumps(self.__dict__, **kwargs)


class JsonDeserializable:
    """Mixin class that allows a class to be deserialized."""

    @classmethod
    def from_json(cls, s, **kwargs):
        """
        Deserialize `cls` from json string.

        :param s: json string
        """
        obj = cls()
        d = json.loads(s, **kwargs)
        obj.__dict__ = d
        return obj


@unique
class Action(IntEnum):
    """
    Actions that a peer must specify in protocol message.

    The reason for defaulting to 1 as the starting number and not 0 is that 0
    is False in a boolean sense, but enum members all evaluate to True.
    """

    INVALID = -1

    PEER_QUERY = 1
    PEER_RESPONSE = 2
    PEER_DEPARTURE = 3
    # LEAVE = 4
    PING_REQUEST = 5
    PING_RESPONSE = 6
    FILE_REQUEST = 7
    FILE_REQUEST_FORWARD = 8
    FILE_REQUEST_ACK = 9
    FILE_TRANSFER = 10
    FILE_TRANSFER_ACK = 11


class Message(JsonSerializable, JsonDeserializable):
    def __init__(self, action=Action.INVALID, encoding=MESSAGE_ENCODING):
        self.action = action
        self.encoding = encoding

    @classmethod
    def from_raw_buffer(cls, buffer, encoding=MESSAGE_ENCODING):
        """
        Deserialize a `Message` from a raw binary buffer.

        :param buffer: deserialize this buffer
        :raises InvalidMessageError: if buffer cannot be deserialized
        """
        buf = buffer.decode(encoding)
        nbytes, data = buf.split('\n')
        msg = cls.from_json(data)
        return msg

    @classmethod
    def from_json(cls, s, **kwargs):
        """
        Deserialize a `Message` from json string.

        :param s: json string
        """
        msg = super().from_json(s, **kwargs)
        if not hasattr(msg, 'action'):
            raise InvalidMessageError(
                'Message must contain an \"action\" field')
        return msg

    def format(self, **kwargs):
        r"""
        Format message to string according to the protocol.

        Format: content_length + '\n' + content
        """
        s = self.json(**kwargs)
        return str(len(s.encode(self.encoding))) + '\n' + s

    def byte_string(self):
        """Format the message into byte string."""
        return bytes(self.format(ensure_ascii=False, separators=(',', ':')),
                     self.encoding)

    def __str__(self):
        return self.json()
