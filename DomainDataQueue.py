import asyncio
from collections import deque

class DomainDataQueue:
    def __init__(self, max_size=10):
        self.queue = deque()
        self.max_size = max_size
        self.lock = asyncio.Lock()

    async def add(self, domain_data):
        async with self.lock:
            self.queue.append(domain_data)
            if len(self.queue) >= self.max_size:
                return await self.flush()
        return False

    async def flush(self):
        async with self.lock:
            data_to_process = list(self.queue)
            self.queue.clear()
            return data_to_process