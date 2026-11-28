"""Exceptions raised by tekmar_482."""


class TekmarError(Exception):
    """Base class for tekmar_482 errors."""


class ProtocolError(TekmarError):
    """Raised when a packet or message violates the tekmar protocol."""


class StockFirmwareError(ProtocolError):
    """A stock-style NullMethod and a valid tHA version identified the application."""


class FirmwareImageUnavailableError(ProtocolError):
    """The installation has no bundled flash image."""


class TransportError(TekmarError):
    """Raised when a transport cannot read or write data."""


class ReportOverflowError(TekmarError):
    """A report subscriber fell behind. Refresh state before resubscribing."""


class ConnectionClosedError(TransportError):
    """Raised when the remote endpoint closes the connection."""


class InventoryDrainTimeoutError(TimeoutError):
    """An abandoned inventory has no terminator; reconnect before starting another."""

    def __init__(self) -> None:
        super().__init__(
            "Timed out draining the previous inventory; reconnect to retry",
        )


class UnknownServiceError(ProtocolError):
    """Raised when a tRPC service name is unknown."""


class UnknownMethodError(ProtocolError):
    """Raised when a tRPC method name is unknown."""


class UnknownFieldError(ProtocolError):
    """Raised when a field does not belong to a known tRPC method."""
