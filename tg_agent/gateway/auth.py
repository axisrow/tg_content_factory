"""Gateway-owned login flows. Neither pending nor authorized sessions leave it."""

import asyncio

from tg_shared.auth import TelegramAuth


class GatewayLogins:
    def __init__(self, store, access):
        self.store = store
        self.access = access
        self.flows = {}
        self.locks = {}

    async def close(self):
        await asyncio.gather(*(auth.cleanup() for auth in self.flows.values()), return_exceptions=True)

    async def materialize(self, project, phone, session):
        phone = phone if phone.startswith("+") else "+" + phone
        account = await self.store.import_account(phone, session)
        current = await self.store.catalog(project)
        await self.store.grant(project, account, primary=not current)
        start = getattr(self.access, "start", None)
        if start is not None:
            await start()
        return {"phone": phone, "state": "success", "is_active": True}

    async def execute(self, project, body):
        async with self.locks.setdefault(project, asyncio.Lock()):
            if body.action == "import":
                from tg_shared.auth import validate_session_string

                if not body.phone or not validate_session_string(body.session_string.get_secret_value()):
                    raise ValueError("Invalid phone/session")
                return await self.materialize(project, body.phone, body.session_string.get_secret_value())
            auth = self.flows.get(project)
            if auth is None:
                source = getattr(self.access, "auth", None)
                if source is None:
                    raise ValueError("Gateway authentication is unavailable")
                auth = TelegramAuth(source.api_id, source.api_hash, proxy=source.proxy)
                self.flows[project] = auth
            if body.action in {"send_code", "resend_code"}:
                # An existing shared session is changed only through explicit administration.
                if any(row["phone"] == body.phone for row in await self.store.catalog()):
                    raise ValueError("Shared account exists; attach it or explicitly replace its session")
                result = await getattr(auth, body.action)(body.phone)
                return {"phone": body.phone, **{key: value for key, value in result.items() if key != "session_str"}}
            if body.action == "verify_code":
                session = await auth.verify_code(body.phone, body.code.get_secret_value(), body.phone_code_hash,
                                                  body.password_2fa.get_secret_value() or None)
                return await self.materialize(project, body.phone, session)
            if body.action == "qr_start":
                return await auth.qr_start()
            if body.action == "qr_password":
                result = await auth.qr_password(body.qr_id, body.password_2fa.get_secret_value())
                if result.get("state") != "success":
                    return result
            snapshot = auth.qr_status(body.qr_id)
            if snapshot is None:
                raise ValueError("QR login expired; start again")
            if snapshot["state"] != "success":
                return snapshot
            consumed = await auth.consume_qr_session(body.qr_id)
            if consumed is None:
                return {"state": "consumed"}
            try:
                return await self.materialize(project, consumed["phone"], consumed["session_string"])
            except Exception:
                auth.qr_fail(body.qr_id, "qr_materialization_failed")
                raise
