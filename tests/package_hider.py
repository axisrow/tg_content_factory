"""A meta-path finder that makes packages import as if not installed (#1720).

Stdlib-only on purpose: ``tests/helpers.hide_packages`` uses it in-process, and
``tests/test_core_without_surfaces.py`` imports it into fresh interpreters that
must not pull the test harness in.

A ``None`` entry in ``sys.modules`` does not model a missing distribution:
``import pkg.sub`` then fails with ``exc.name == "pkg.sub"``, whereas an absent
package fails with ``exc.name == "pkg"`` — the case the CLI's optional-surface
loaders (tg_webui, tg_tui) tell apart from a broken import inside an installed
package. This finder raises exactly that error.
"""

from __future__ import annotations

import importlib.abc
from collections.abc import Iterable


class PackageHider(importlib.abc.MetaPathFinder):
    def __init__(self, names: Iterable[str]) -> None:
        self.names = frozenset(names)

    def find_spec(self, fullname, path=None, target=None):
        top = fullname.partition(".")[0]
        if top in self.names:
            raise ModuleNotFoundError(f"No module named {top!r}", name=top)
        return None
