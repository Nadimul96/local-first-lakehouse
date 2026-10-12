"""Exception hierarchy. Every failure is explicit; nothing degrades silently."""


class LakehouseError(RuntimeError):
    """Base class for pipeline failures that callers are expected to handle."""


class ConfigError(LakehouseError):
    """Configuration is missing or unsafe (for example, a world-readable key file)."""


class DependencyMissingError(LakehouseError):
    """An optional dependency (boto3, psycopg) is needed for the selected provider."""


class IntegrityError(LakehouseError):
    """Stored bytes no longer match the hash or sequence range recorded for them."""


class ConflictError(LakehouseError):
    """An idempotency key was reused for different content."""


class VerificationError(LakehouseError):
    """A remote object failed read-back verification (size, hash, or GCM tag)."""


class CapacityError(LakehouseError):
    """A bounded local store refused a write to apply backpressure."""


class LeaseLostError(LakehouseError):
    """A queue worker tried to act on an item it no longer holds the lease for."""


class WriterBusyError(LakehouseError):
    """Another process already holds the single-writer lock for this data root."""
