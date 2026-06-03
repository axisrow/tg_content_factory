from __future__ import annotations

import pytest

pytestmark = pytest.mark.real_tg_never


@pytest.mark.skip(reason="live account deletion is intentionally not tested")
def test_account_delete_live_user_is_blocked():
    # `account delete` removes a user's configured Telegram account/session from
    # the live project database. We do not plan to test that operation on a live
    # user account until there is a dedicated disposable-account workflow.
    pass
