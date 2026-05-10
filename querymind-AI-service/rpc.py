from querymind_shared.rpc_client import RPCClient
from core.config import settings

_rpc_client: RPCClient | None = None


def get_rpc_client() -> RPCClient:
    global _rpc_client
    if _rpc_client is None:
        _rpc_client = RPCClient(amqp_url=settings.AMQP_URL)
    return _rpc_client


def reset_rpc_client() -> None:
    """Call this when the connection needs to be re-established."""
    global _rpc_client
    if _rpc_client is not None:
        try:
            _rpc_client.close()
        except Exception:
            pass
    _rpc_client = None
