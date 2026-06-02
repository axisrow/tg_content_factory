# CLI Dangerous-Operations Testing Guide

> **Disclaimer:** Operations described here write to real Telegram accounts.
> They may create channels, send messages, delete messages, send authentication
> codes via SMS, or interact with BotFather.  Always use a dedicated test
> account — never your primary personal account.

---

## Automated (gated) tests

The following operations have automated pytest tests in
`tests/cli_real_tg_integration/manual/`.  They are skipped by default and only
run when both gate environment variables are set:

```
RUN_CLI_REAL_TG_LIVE=1 RUN_REAL_TELEGRAM_MANUAL=1 pytest tests/cli_real_tg_integration/manual/ -v
```

| Test file | Operation | Cleanup |
|---|---|---|
| `test_dialogs_create_channel_cleanup.py` | Create private broadcast channel | Leave (delete) via `dialogs leave` |
| `test_dialogs_delete_message_scratch.py` | Send scratch message then delete it | Self-contained — delete is the test |

### Test account requirements

- A connected Telegram account in the live database (check with `account list`).
- For `test_dialogs_delete_message_scratch`: a self-owned dialog cached in
  `dialog_cache` (set `CLI_REAL_TG_MUTATION_CHAT` to a numeric ID or
  `@username` to target a specific chat, or cache an own live dialog first).

---

## Manual-only runbooks

The operations below have **no automated tests**.  Run them manually in a
sandbox environment and follow the rollback steps before finishing.

### Setup: sandbox environment

1. Use a dedicated test Telegram account (separate SIM or virtual number).
2. Set `CLI_REAL_TG_ROOT` to the project root containing the live `config.yaml`
   and database.
3. Confirm the account is connected: `python -m src.main account list`.

---

### SMS authentication (`account send-code` / `verify-code`)

**Risk:** Sends a real SMS to the phone number.  Cannot be cancelled once sent.

**Steps:**

```bash
# 1. Request the code
python -m src.main account send-code --phone +1XXXXXXXXXX

# 2. Enter the code received via SMS
python -m src.main account verify-code --phone +1XXXXXXXXXX --code 12345
```

**Rollback:** If the session is not wanted, delete it:

```bash
python -m src.main account delete --phone +1XXXXXXXXXX --yes
```

---

### Admin rights mutation (`dialogs edit-admin`)

**Risk:** Grants or revokes admin rights in a group/supergroup.  Requires a
second participant in the sandbox group.

**Prerequisites:**

1. A sandbox supergroup or group created for testing.
2. A second test user ID (numeric Telegram user ID) who is already a member.

**Steps:**

```bash
# Grant admin rights (custom title, can-post)
python -m src.main dialogs edit-admin \
  --phone +1XXXXXXXXXX \
  --chat-id <GROUP_ID> \
  --user-id <TARGET_USER_ID> \
  --title "Test Admin" \
  --yes

# Verify with participants list
python -m src.main dialogs participants --phone +1XXXXXXXXXX <GROUP_ID>
```

**Rollback:** Remove admin rights:

```bash
python -m src.main dialogs edit-admin \
  --phone +1XXXXXXXXXX \
  --chat-id <GROUP_ID> \
  --user-id <TARGET_USER_ID> \
  --demote \
  --yes
```

---

### Permissions mutation (`dialogs edit-permissions`)

**Risk:** Changes send/media permissions for all members of a group.

**Prerequisites:** A sandbox group where you are the creator.

**Steps:**

```bash
# Restrict all members from sending messages
python -m src.main dialogs edit-permissions \
  --phone +1XXXXXXXXXX \
  --chat-id <GROUP_ID> \
  --no-send-messages \
  --yes

# Verify effect
python -m src.main dialogs participants --phone +1XXXXXXXXXX <GROUP_ID>
```

**Rollback:** Restore default permissions:

```bash
python -m src.main dialogs edit-permissions \
  --phone +1XXXXXXXXXX \
  --chat-id <GROUP_ID> \
  --send-messages \
  --yes
```

---

### Kick participant (`dialogs kick`)

**Risk:** Removes a participant from a group.

**Prerequisites:** A sandbox group with a second test account as member.

**Steps:**

```bash
python -m src.main dialogs kick \
  --phone +1XXXXXXXXXX \
  --chat-id <GROUP_ID> \
  --user-id <TARGET_USER_ID> \
  --yes
```

**Rollback:** Re-invite the kicked user (add them back via Telegram client or
via an invite link).

---

### Notification bot setup (`notification setup` / `notification delete`)

**Risk:** Creates a real Telegram bot via BotFather interaction.  Requires the
connected account to initiate a BotFather session.

**Prerequisites:** A connected Telegram account with BotFather accessible.

**Steps:**

```bash
# Interactive setup — follow BotFather prompts
python -m src.main notification setup --phone +1XXXXXXXXXX

# Verify the bot is registered
python -m src.main notification status
```

**Rollback:** Delete the notification bot configuration:

```bash
python -m src.main notification delete --phone +1XXXXXXXXXX --yes
```

> To also delete the bot from Telegram, open a chat with @BotFather and use
> `/deletebot`, then select the bot created during the test.

---

## Verifying results and rollback checklist

After each manual scenario, confirm state is clean:

- [ ] `account list` — no unexpected accounts remain.
- [ ] `dialogs list` — test channels/groups are left or deleted.
- [ ] `notification status` — no stale bot configurations.
- [ ] Check Telegram client directly for any artefacts (test channels, test
      messages, admin badges).
