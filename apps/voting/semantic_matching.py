"""Bounded, exact cosine retrieval over precomputed OpenAI song embeddings.

This uses no new infrastructure: vectors are stored in PostgreSQL (JSON)
and searched only when lexical retrieval is weak. At current station
catalogue sizes, an exact scan avoids pgvector extension dependency.
Vector similarity is a *retrieval signal*, NEVER an approval probability.
"""
import hashlib
import heapq
import logging
import math

import requests
from django.conf import settings
from django.core.cache import cache

from .models import SongEmbedding

logger = logging.getLogger(__name__)

EMBEDDINGS_ENDPOINT = "https://api.openai.com/v1/embeddings"


def embedding_text(song):
    from .catalogue_matching import normalise
    return f"{normalise(song.artist)} - {normalise(song.title)}"


def fingerprint(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_vector(value):
    if not isinstance(value, list) or not 16 <= len(value) <= 4096:
        raise ValueError("Unexpected embedding vector dimension")
    values = []
    for item in value:
        if type(item) not in (float, int) or not math.isfinite(item):
            raise ValueError("Non-finite embedding component")
        values.append(float(item))
    norm = math.sqrt(sum(v * v for v in values))
    if not 0.1 <= norm <= 100:
        raise ValueError("Invalid embedding magnitude")
    return [v / norm for v in values]


def embed_texts(texts):
    """One bounded API request for a batch; credentials never leave the backend."""
    if not texts or len(texts) > 64:
        raise ValueError("Embedding batch must contain between 1 and 64 items")
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise RuntimeError("Embedding API key is not configured")
    model = settings.OPENAI_EMBEDDING_MODEL
    response = requests.post(
        EMBEDDINGS_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "input": texts, "encoding_format": "float"},
        timeout=(5, 35),
    )
    response.raise_for_status()
    data = response.json().get("data")
    if not isinstance(data, list) or len(data) != len(texts):
        raise ValueError("Incomplete embeddings response")
    embeddings = {}
    for record in data:
        index = record.get("index")
        if type(index) is not int or index in embeddings or not 0 <= index < len(texts):
            raise ValueError("Invalid embeddings index")
        embeddings[index] = _validate_vector(record.get("embedding"))
    if len({len(vector) for vector in embeddings.values()}) != 1:
        raise ValueError("Mixed embedding dimensions")
    return [embeddings[i] for i in range(len(texts))]


def semantic_candidates(station, artist, title, *, limit=8, exclude_ids=()):
    """Top-K exact cosine candidates, only when vector search is enabled.

    A missing embedding corpus or API outage never blocks ordinary votes.
    The output may be supplied to OpenAI but cannot approve a vote alone.
    """
    if not settings.AI_VECTOR_ENABLED:
        return []
    from .catalogue_matching import normalise, strip_vote_prefix
    query = " ".join(part for part in (artist, title) if normalise(part) != "unknown artist")
    query = strip_vote_prefix(normalise(query))
    if len(query) < 3:
        return []
    model = settings.OPENAI_EMBEDDING_MODEL
    records = SongEmbedding.objects.filter(
        station=station, model=model, song__station=station,
        song__status="verified",
    ).select_related("song").iterator(chunk_size=100)
    key = "airvote:embedding:" + fingerprint(model + "|" + query)
    vector = cache.get(key)
    if vector is None:
        try:
            vector = embed_texts([query])[0]
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            logger.warning("Semantic retrieval unavailable (%s)", type(exc).__name__)
            return []
        cache.set(key, vector, 600)
    excluded = set(exclude_ids)
    ranked = []
    for row in records:
        if row.song_id in excluded:
            continue
        if row.text_fingerprint != fingerprint(embedding_text(row.song)):
            continue
        try:
            other = row.vector
            if len(other) != len(vector):
                continue
            similarity = sum(x * y for x, y in zip(vector, other))
        except (ValueError, TypeError):
            continue
        if similarity >= settings.AI_VECTOR_RETRIEVAL_MIN_SIMILARITY:
            ranked.append((similarity, row.song))
    return heapq.nlargest(limit, ranked, key=lambda x: (x[0], -x[1].pk))
