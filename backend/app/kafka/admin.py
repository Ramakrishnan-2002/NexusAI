import asyncio
from typing import Dict, List, Optional
from confluent_kafka.admin import AdminClient, NewTopic
from app.core.config import settings
from app.core.logging import logger
from app.kafka.topics import ALL_TOPICS


class KafkaAdminService:
    """
    Kafka Cluster Administrator backed by confluent-kafka AdminClient:
      - Topic creation & partition management
      - Metadata inspection
      - Cluster health checks
    """
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self._admin: Optional[AdminClient] = None

    def _get_admin(self) -> AdminClient:
        if self._admin is None:
            self._admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        return self._admin

    async def list_topics(self) -> List[str]:
        try:
            admin = self._get_admin()
            metadata = await asyncio.to_thread(admin.list_topics, timeout=5.0)
            return list(metadata.topics.keys())
        except Exception as e:
            logger.error(f"Failed listing Kafka topics: {e}")
            return []

    async def ensure_topics(self, topics: Optional[List[str]] = None, num_partitions: int = 3, replication_factor: int = 1):
        """Idempotently ensure all required business topics exist in the Kafka cluster"""
        topics_to_create = topics or ALL_TOPICS
        try:
            admin = self._get_admin()
            metadata = await asyncio.to_thread(admin.list_topics, timeout=5.0)
            existing = set(metadata.topics.keys())

            new_topics = [
                NewTopic(
                    topic=t,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                for t in topics_to_create
                if t not in existing
            ]

            if new_topics:
                fs = admin.create_topics(new_topics)
                for topic, f in fs.items():
                    try:
                        await asyncio.to_thread(f.result)
                        logger.info(f"Created Kafka topic '{topic}' (partitions: {num_partitions})")
                    except Exception as exc:
                        logger.warning(f"Topic creation info for '{topic}': {exc}")
        except Exception as e:
            logger.warning(f"Failed ensuring Kafka topics: {e}")


kafka_admin_service = KafkaAdminService()
