"""Exceptions raised by tekmar_482."""


class TekmarError(Exception):
    """Base class for tekmar_482 errors."""


class ProtocolError(TekmarError):
    """Raised when a packet or message violates the tekmar protocol."""


class TransportError(TekmarError):
    """Raised when a transport cannot read or write data."""


class ConnectionClosedError(TransportError):
    """Raised when the remote endpoint closes the connection."""


class UnknownServiceError(ProtocolError):
    """Raised when a tRPC service name is unknown."""


class UnknownMethodError(ProtocolError):
    """Raised when a tRPC method name is unknown."""


class UnknownFieldError(ProtocolError):
    """Raised when a field does not belong to a known tRPC method."""
