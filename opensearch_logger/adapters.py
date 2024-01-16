import asyncio
import queue
import threading

from opensearchpy import AsyncOpenSearch, helpers

class AsyncOpenSearchAdapter:
    """
    Wrapper for async OpenSearch which returns immediately and schedules activity on a separate asyncio event loop.
    """
    def __init__(self, get_client_fn):
        self._get_client_fn = get_client_fn

        self._q = queue.Queue()

        self._thread = threading.Thread(target=self._asyncio_client_thread, daemon=True)
        self._thread.start()

    def _asyncio_client_thread(self):
        loop = asyncio.new_event_loop()

        while True:
            cmd, args, kwargs = self._q.get()

            client = loop.run_until_complete(self._get_client_fn())

            if not client:
                raise RuntimeError("_get_client_fn returned None instead of an AsyncOpenSearch instance")

            match cmd:
                case 'bulk':
                    loop.run_until_complete(helpers.async_bulk(*args, client=client, **kwargs))
                case _:
                    continue

    def bulk(self, *args, **kwargs):
        self._q.put(('bulk', args, kwargs))
