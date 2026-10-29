"""Native operations always borrow the gateway's persistent connection."""

from tg_agent.telegram.client_pool import ClientPool


class GatewayClientPool(ClientPool):
    async def _acquire_from_lease(self, account_lease, *, force_native=False, **options):
        # Legacy callers may request an isolated force-native lease. In shared
        # mode connection ownership stays with the gateway for every operation.
        return await super()._acquire_from_lease(account_lease, force_native=False, **options)
