"""Read-only, bounded AI matching. All mutations belong to pipeline.py."""
import json
import logging
import requests
from typing import List, Dict

from django.conf import settings

from .models import (
    CleanedSong,
)
from apps.accounts.models import Station

logger = logging.getLogger(__name__)

# OpenAI API settings
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"

# Maximum verified songs to include in prompt
MAX_SONGS_IN_PROMPT = 500


def get_openai_api_key() -> str:
    """Get OpenAI API key from settings."""
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured in settings")
    return api_key


def call_openai_api(system_prompt: str, user_prompt: str) -> str:
    """
    Call OpenAI GPT-4o-mini API and return the response text.

    Args:
        system_prompt: System message for the model
        user_prompt: User message/query

    Returns:
        The model's response text
    """
    api_key = get_openai_api_key()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": settings.OPENAI_MODEL,
        "max_tokens": 2048,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.0,  # Deterministic for matching
    }

    try:
        response = requests.post(
            OPENAI_API_URL, headers=headers, json=payload, timeout=(5, 25)
        )
        response.raise_for_status()

        data = response.json()
        choices = data.get("choices", [])
        if choices and len(choices) > 0:
            return choices[0].get("message", {}).get("content", "")
        return ""
    except requests.exceptions.RequestException as e:
        logger.warning("OpenAI request failed (%s)", type(e).__name__)
        raise


# System prompt for vote matching
MATCHING_SYSTEM_PROMPT = """You are a music database matching assistant for Radio Zimbabwe.

Your task is to match incoming song votes against a list of VERIFIED songs in the database.

RULES:
1. You will receive an incoming vote in format "Artist - Song"
2. You will receive a list of verified songs to match against
3. Match ONLY if you are HIGHLY CONFIDENT (95%+) the vote refers to a verified song
4. Handle common variations:
   - Slight spelling differences: "Winky D" vs "Winkyd"
   - Case differences: "IJIPITA" vs "Ijipita"
   - Minor typos: "Jah Prayza" vs "Jah Prayzah"
   - "ft", "feat", "featuring" variations
5. DO NOT GUESS. If you're not highly confident, return no match.
6. A vote must clearly refer to the same artist AND same song to match.

RESPONSE FORMAT (JSON only, no other text):
{
  "matched": true/false,
  "matched_song_id": <id or null>,
  "matched_song_name": "<canonical name or null>",
  "confidence": "high"/"medium"/"low"/"none",
  "reasoning": "<brief explanation>"
}

If matched=true, confidence MUST be "high". Otherwise we don't auto-merge."""


def get_verified_songs_for_prompt(station: str = None) -> List[Dict]:
    """
    Get list of verified songs formatted for the LLM prompt.

    Args:
        station: Station code to filter by. If None, uses Radio Zimbabwe.

    Returns:
        List of dicts with id, artist, title, canonical_name
    """
    if station is None:
        station = Station.RADIO_ZIMBABWE

    songs = CleanedSong.objects.filter(station=station, status="verified").order_by(
        "artist", "title"
    )[:MAX_SONGS_IN_PROMPT]

    return [
        {
            "id": song.id,
            "artist": song.artist,
            "title": song.title,
            "canonical_name": song.canonical_name,
        }
        for song in songs
    ]


def format_songs_for_prompt(songs: List[Dict]) -> str:
    """Format the songs list for inclusion in the prompt."""
    if not songs:
        return "No verified songs in database."

    lines = []
    for song in songs:
        lines.append(f"ID:{song['id']} | {song['canonical_name']}")

    return "\n".join(lines)


def match_vote_with_llm(artist: str, title: str, verified_songs: List[Dict]) -> Dict:
    """
    Use GPT-4o-mini to match a vote against verified songs.

    Args:
        artist: Artist name from the vote
        title: Song title from the vote
        verified_songs: List of verified songs to match against

    Returns:
        Dict with matched, matched_song_id, matched_song_name, confidence, reasoning
    """
    if not verified_songs:
        return {
            "matched": False,
            "matched_song_id": None,
            "matched_song_name": None,
            "confidence": "none",
            "reasoning": "No verified songs in database to match against",
        }

    vote_text = f"{artist} - {title}"
    songs_text = format_songs_for_prompt(verified_songs)

    user_prompt = f"""INCOMING VOTE: {vote_text}

VERIFIED SONGS DATABASE:
{songs_text}

Match the incoming vote to a verified song. Return JSON only."""

    try:
        response_text = call_openai_api(MATCHING_SYSTEM_PROMPT, user_prompt)

        # Parse JSON response
        # Clean up response if it has markdown code blocks
        response_text = response_text.strip()
        if response_text.startswith("```"):
            # Remove markdown code block
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])

        result = json.loads(response_text)

        # Validate response structure
        if not isinstance(result, dict):
            raise ValueError("Response is not a dict")

        # Ensure required fields
        return {
            "matched": result.get("matched") is True
            and result.get("confidence") == "high",
            "matched_song_id": result.get("matched_song_id"),
            "matched_song_name": result.get("matched_song_name"),
            "confidence": result.get("confidence", "none"),
            "reasoning": result.get("reasoning", "No reasoning provided"),
        }

    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse LLM response: {e}")
        return {
            "matched": False,
            "matched_song_id": None,
            "matched_song_name": None,
            "confidence": "none",
            "reasoning": f"LLM response parsing error: {str(e)}",
        }
    except Exception as e:
        logger.warning("AI matching failed (%s)", type(e).__name__)
        return {
            "matched": False,
            "matched_song_id": None,
            "matched_song_name": None,
            "confidence": "none",
            "reasoning": f"LLM error: {type(e).__name__}",
        }
