"""Session store that keeps one current entity row per peer (#1843).

Telethon's ``MemorySession`` — the base of ``StringSession`` — stores entities
as a set of ``(id, hash, username, phone, name)`` tuples and only ever adds to
it. Two consequences for a long-lived client:

* a peer whose title or username changed leaves its older rows behind;
* when a username moves from chat A to chat B both rows carry it, and the
  lookup by username returns whichever the set yields first.

Writes resolve cached usernames from this store without asking Telegram, so
the second case could deliver a message to the username's previous owner.
Telethon's ``SQLiteSession`` already evicts the older owner of a username;
``MemorySession`` has no equivalent, hence this subclass.
"""

from __future__ import annotations

from typing import Any

from telethon.sessions import StringSession

# Row layout shared with Telethon's MemorySession.
_ID, _HASH, _USERNAME, _PHONE, _NAME = range(5)


class FreshEntityStringSession(StringSession):
    """``StringSession`` whose entity store replaces rows instead of piling them up.

    Invariants after every ``process_entities`` call:

    * at most one row per peer id — the newest;
    * at most one row per username — its current owner. The previous owner
      keeps its id and access hash (it stays resolvable by id) and loses only
      the username.

    Two dictionaries index the set by id and by username, so the common case —
    entities that are already known and unchanged — costs one lookup per row
    and never rescans the store. Lookups themselves stay Telethon's.
    """

    def __init__(self, string: str | None = None) -> None:
        super().__init__(string)
        self._row_by_id: dict[Any, tuple] = {}
        self._row_by_username: dict[str, tuple] = {}

    def process_entities(self, tlo: Any) -> None:
        """Merge the entities of a response, newest row winning."""
        for row in self._entities_to_rows(tlo):
            self._store_row(row)

    def _store_row(self, row: tuple) -> None:
        current = self._indexed(self._row_by_id, row[_ID])
        if current == row:
            return
        if current is not None:
            self._drop(current)
        username = row[_USERNAME]
        if username is not None:
            holder = self._indexed(self._row_by_username, username)
            if holder is not None and holder[_ID] != row[_ID]:
                # The username moved to this peer: its previous owner stays
                # known by id, without the name.
                self._drop(holder)
                self._add((holder[_ID], holder[_HASH], None, holder[_PHONE], holder[_NAME]))
        self._add(row)

    def _indexed(self, index: dict, key: Any) -> tuple | None:
        """The indexed row, ignoring an entry the entity set no longer holds."""
        row = index.get(key)
        return row if row is not None and row in self._entities else None

    def _add(self, row: tuple) -> None:
        self._entities.add(row)
        self._row_by_id[row[_ID]] = row
        if row[_USERNAME] is not None:
            self._row_by_username[row[_USERNAME]] = row

    def _drop(self, row: tuple) -> None:
        self._entities.discard(row)
        if self._row_by_id.get(row[_ID]) == row:
            del self._row_by_id[row[_ID]]
        if row[_USERNAME] is not None and self._row_by_username.get(row[_USERNAME]) == row:
            del self._row_by_username[row[_USERNAME]]
