import asyncio
import queue
import threading

from opensearchpy import helpers, exceptions

class AsyncOpenSearchAdapter:
    """
    Wrapper for async OpenSearch which returns immediately and schedules activity on a separate asyncio event loop.
    """
    def __init__(self, get_client_fn):
        self._get_client_fn = get_client_fn

        self._q = queue.Queue()

        self._thread = threading.Thread(target=self._asyncio_client_thread_entrypoint, daemon=True)
        self._thread.start()

    def _asyncio_client_thread_entrypoint(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._asyncio_client_thread())

    async def _asyncio_client_thread(self):
        while True:
            cmd, args, kwargs = self._q.get()

            try:
                await self.process_cmd_with_retry(cmd, args, kwargs)
            except Exception as e:
                print('OpenSearch adapter error', e)

    async def process_cmd_with_retry(self, cmd, args, kwargs):
        client = await self._get_client_fn()

        if not client:
            raise RuntimeError("_get_client_fn returned None instead of an AsyncOpenSearch instance")

        # Attempt to perform the command. If the first attempt produces a transport error, retry with a
        # new client.
        try:
            return await self._process_cmd(client, cmd, args, kwargs)
        except exceptions.TransportError:
            client = await self._get_client_fn(transport_error=True)
            return await self._process_cmd(client, cmd, args, kwargs)

    async def _process_cmd(self, client, cmd, args, kwargs):
        match cmd:
            case 'bulk':
                return await self.cmd_bulk(client, *args, **kwargs)
            case 'raw_ping':
                return await self.cmd_raw_ping(client)
            case _:
                return

    async def cmd_bulk(self, client, *args, **kwargs):
        return await helpers.async_bulk(*args, client=client, **kwargs)

    async def cmd_raw_ping(self, client):
        """
        Do a ping which either succeeds or raises TransportError.
        """
        return await client.transport.perform_request(
            "HEAD", "/", params=None, headers=None
        )

    def bulk(self, *args, **kwargs):
        self._q.put(('bulk', args, kwargs))

    def ping(self):
        self._q.put(('ping', [], {}))
