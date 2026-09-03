from typing import Any, Dict
from app.core.config import settings
from app.core.logging import logger
from app.db.session import AsyncSessionLocal
from app.kafka.consumer import EventConsumer
from app.kafka.producer import event_producer
from app.models.knowledge_chunk import KnowledgeChunk
from app.schemas.event import ProcessedArticleEvent
from app.search.embeddings import embedding_service
from app.rag.chunker import KnowledgeChunker


class EmbeddingWorker:
    """
    Embedding Worker:
      1. Consumes 'wikimedia.article.processed' events
      2. Extracts structured knowledge representation from the edit
      3. Generates 384-dimensional dense semantic vector embeddings
      4. Persists KnowledgeChunk to PostgreSQL + pgvector
      5. Emits 'wikimedia.embedding.created'
    """

    def __init__(self):
        self.consumer = EventConsumer(
            topics=[settings.TOPIC_ARTICLE_PROCESSED],
            group_id="embedding",
        )
        self._running = False

    async def handle_event(self, event_data: Dict[str, Any]):
        evt = ProcessedArticleEvent.model_validate(event_data)

        # Generate text representation to embed
        comment_str = f" Comment: '{evt.comment}'." if evt.comment else ""
        text_content = (
            f"Wikipedia article '{evt.article_title}' was edited by user '{evt.editor_username}' "
            f"with byte difference of {evt.byte_diff:+d} bytes.{comment_str}"
        )

        # Compute semantic vector embedding
        vector = embedding_service.get_embedding(text_content)

        async with AsyncSessionLocal() as session:
            chunk = KnowledgeChunk(
                article_id=evt.article_id,
                edit_id=evt.edit_id,
                revision_id=evt.revision_id,
                article_title=evt.article_title,
                title=f"Edit on {evt.article_title} ({evt.wiki})",
                content=text_content,
                embedding=vector,
                chunk_metadata={
                    "wiki": evt.wiki,
                    "namespace": evt.namespace,
                    "editor": evt.editor_username,
                    "byte_diff": evt.byte_diff,
                    "is_minor": evt.is_minor,
                    "is_bot": evt.is_bot,
                },
                occurred_at=evt.occurred_at,
            )
            session.add(chunk)
            await session.commit()

        # Emit embedding created event
        await event_producer.send(
            topic=settings.TOPIC_EMBEDDING_CREATED,
            value={
                "chunk_id": chunk.id,
                "article_id": evt.article_id,
                "article_title": evt.article_title,
                "embedding_dim": len(vector),
            },
            key=evt.article_title,
        )

    async def run(self):
        self._running = True
        logger.info("Starting Embedding Worker...")
        await event_producer.start()
        await self.consumer.consume_loop(self.handle_event)

    async def stop(self):
        self._running = False
        await self.consumer.stop()
