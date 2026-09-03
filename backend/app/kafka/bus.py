import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional
from app.core.logging import logger


class InMemoryEventBus:
    """
    In-memory pub/sub broker for development, testing, and fallback.
    Maintains per-topic queues and supports subscriber fan-out.
    """
    def __init__(self):
        self._queues: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._all_messages: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def register_subscriber(self, topic: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._queues[topic].append(q)
        return q

    def unregister_subscriber(self, topic: str, q: asyncio.Queue):
        if topic in self._queues and q in self._queues[topic]:
            self._queues[topic].remove(q)

    async def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        self._all_messages[topic].append(message)
        subscribers = self._queues.get(topic, [])
        for q in subscribers:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                logger.warning(f"In-memory queue for topic {topic} full; dropping oldest.")
                try:
                    q.get_nowait()
                    q.put_nowait(message)
                except Exception:
                    pass

    def get_published_messages(self, topic: str) -> List[Dict[str, Any]]:
        return list(self._all_messages.get(topic, []))

    def clear(self):
        self._queues.clear()
        self._all_messages.clear()


global_event_bus = InMemoryEventBus()
