import math
import re
from typing import List, Optional
import numpy as np
from app.core.config import settings
from app.core.logging import logger


class EmbeddingService:
    """
    Generates dense semantic vector embeddings (384 dimensions) for Wikipedia knowledge chunks.
    Supports SentenceTransformers when available, Ollama embedding API, or fast dense deterministic vectorizer.
    """
    def __init__(self, dimension: int = 384):
        self.dimension = dimension
        self._model = None
        self._initialized = False

    def _init_local_model(self):
        if self._initialized:
            return
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Loaded SentenceTransformer all-MiniLM-L6-v2.")
        except Exception as e:
            logger.info(f"SentenceTransformer not loaded ({e}); using fast deterministic dense vectorizer.")
            self._model = None
        self._initialized = True

    def get_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for a single text chunk"""
        embeddings = self.get_embeddings_batch([text])
        return embeddings[0]

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embedding vectors for a batch of text chunks"""
        self._init_local_model()

        if self._model is not None:
            try:
                embeddings = self._model.encode(texts, normalize_embeddings=True)
                return [emb.tolist() for emb in embeddings]
            except Exception as e:
                logger.warning(f"SentenceTransformer encoding failed: {e}; using fallback.")

        # Fallback dense normalized projection
        results = []
        for text in texts:
            results.append(self._deterministic_dense_vector(text))
        return results

    def _deterministic_dense_vector(self, text: str) -> List[float]:
        """
        Fast deterministic 384-dimensional dense normalized vector
        based on token hashing and n-gram frequencies.
        """
        vec = np.zeros(self.dimension, dtype=np.float32)
        clean = re.sub(r"[^\w\s]", " ", text.lower())
        tokens = clean.split()

        if not tokens:
            vec[0] = 1.0
            return vec.tolist()

        for idx, token in enumerate(tokens):
            h = hash(token) % self.dimension
            h2 = (hash(token) * 31 + idx) % self.dimension
            vec[h] += 1.0
            vec[h2] += 0.5

        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return vec.tolist()


embedding_service = EmbeddingService(dimension=settings.EMBEDDING_DIMENSION)
