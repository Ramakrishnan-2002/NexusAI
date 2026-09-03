import re
from typing import Any, Dict, List
from app.schemas.event import IngestedEvent


class KnowledgeChunker:
    """
    Extracts structured, semantically meaningful knowledge chunks
    from raw/processed Wikipedia events and edit summaries.
    Avoids indexing meaningless JSON noise.
    """

    @staticmethod
    def chunk_event(event: IngestedEvent, article_id: int) -> List[Dict[str, Any]]:
        chunks = []

        # 1. Summary Chunk: Combines Article Title, Editor action, Diff size, and Comment
        action_verb = "expanded" if event.byte_diff > 200 else ("reduced" if event.byte_diff < -200 else "updated")
        comment_text = f" Summary: '{event.comment}'." if event.comment else ""
        
        content = (
            f"Article '{event.article_title}' was {action_verb} by editor '{event.editor_username}' "
            f"with a net change of {event.byte_diff:+d} bytes (total size {event.change_size} bytes)."
            f"{comment_text}"
        )

        chunks.append({
            "article_id": article_id,
            "revision_id": event.revision_id,
            "article_title": event.article_title,
            "title": f"Edit on {event.article_title} ({event.wiki})",
            "content": content,
            "chunk_metadata": {
                "wiki": event.wiki,
                "namespace": event.namespace,
                "editor": event.editor_username,
                "byte_diff": event.byte_diff,
                "is_bot": event.is_bot,
                "is_minor": event.is_minor,
                "occurred_at": event.occurred_at.isoformat(),
            },
            "occurred_at": event.occurred_at,
        })

        return chunks
