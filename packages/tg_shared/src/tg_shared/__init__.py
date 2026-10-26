"""tg-shared — shared Telegram core for tg-agent (factory) and tg-messenger.

Stage 1.1 (#1420): account model, storage Protocol, JSON store.
Wave A (#1421): zero-coupling Telegram modules moved in — ``auth``,
``session_materializer``, ``botfather``, ``identity``, ``media``.
Wave B (#1422): flood protection follows. The package works without the
factory at all.
"""

from .json_store import JsonFileStore
from .models import SharedAccount
from .storage import SharedAccountStore

__version__ = "0.2.0"

__all__ = ["JsonFileStore", "SharedAccount", "SharedAccountStore", "__version__"]
