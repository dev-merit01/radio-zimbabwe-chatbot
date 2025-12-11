"""
LLM-powered song matching using Google Gemini.

This module uses Gemini 1.5 Flash to:
1. Correct misspellings in artist/song names
2. Understand Zimbabwean music context
3. Provide better search queries for Spotify
"""
import json
import logging
from typing import Optional, Tuple

import google.generativeai as genai
from django.conf import settings

logger = logging.getLogger(__name__)

_model = None

# System prompt for Gemini
SYSTEM_PROMPT = """You are a Zimbabwean music expert assistant. Your job is to ONLY fix obvious spelling mistakes in artist and song names.

You know:
- Zimdancehall: Winky D, Holy Ten, Enzo Ishall, Jah Signal, Freeman, Killer T, Tocky Vibes, Nutty O, Ti Gonzi, Voltz JT
- Sungura: Alick Macheso, Suluman Chimbetu, Leonard Dembo, Oliver Mtukudzi
- Gospel: Janet Manyowa, Mathias Mhere, Minister Michael Mahendere
- Contemporary: Jah Prayzah, Ammara Brown, Sha Sha, ExQ, Takura

RULES:
1. ONLY fix clear spelling mistakes (e.g., "winkyd" -> "Winky D", "jah prayza" -> "Jah Prayzah")
2. NEVER change a song title to a different song - keep the song title mostly as-is
3. If you don't recognize a song title, KEEP IT UNCHANGED except for fixing obvious typos
4. Focus on artist name corrections - those are most important
5. Set confidence to "low" if you're not 100% sure

IMPORTANT: Always respond with ONLY valid JSON, no other text."""

USER_PROMPT_TEMPLATE = """Fix spelling mistakes in this song vote:
Artist: "{artist}"
Song: "{title}"

ONLY fix obvious typos. Do NOT change to a different song.

Respond with ONLY this JSON (no markdown):
{{
    "corrected_artist": "Fixed artist spelling (or same if unsure)",
    "corrected_title": "Fixed song spelling (keep similar to original)",
    "confidence": "high" or "medium" or "low",
    "is_zimbabwean": true or false,
    "notes": ""
}}"""


class GeminiNotConfiguredError(RuntimeError):
    """Raised when Gemini API key is missing."""


def _get_model():
    """Get or create the Gemini model instance."""
    global _model
    # Always recreate to pick up prompt changes during dev
    api_key = getattr(settings, 'GEMINI_API_KEY', '')
    if not api_key:
        raise GeminiNotConfiguredError('Gemini API key is not configured.')
    
    genai.configure(api_key=api_key)
    _model = genai.GenerativeModel(
        model_name='gemini-2.0-flash',
        generation_config={
            'temperature': 0.0,  # Zero temperature for deterministic spelling fixes
            'top_p': 0.8,
            'max_output_tokens': 256,
        },
        system_instruction=SYSTEM_PROMPT,
    )
    return _model


def correct_song_query(artist: str, title: str) -> Tuple[str, str, dict]:
    """
    Use Gemini to correct spelling and identify the actual song.
    
    Args:
        artist: User-provided artist name (possibly misspelled)
        title: User-provided song title (possibly misspelled)
    
    Returns:
        Tuple of (corrected_artist, corrected_title, metadata)
        metadata contains: confidence, is_zimbabwean, notes
    """
    try:
        model = _get_model()
    except GeminiNotConfiguredError:
        logger.warning('Gemini not configured, returning original input')
        return artist, title, {'confidence': 'low', 'is_zimbabwean': False, 'notes': ''}
    
    prompt = USER_PROMPT_TEMPLATE.format(artist=artist, title=title)
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Clean up response - remove markdown code blocks if present
        if response_text.startswith('```'):
            lines = response_text.split('\n')
            # Remove first and last lines (```json and ```)
            response_text = '\n'.join(lines[1:-1])
        
        # Parse JSON response
        result = json.loads(response_text)
        
        corrected_artist = result.get('corrected_artist', artist)
        corrected_title = result.get('corrected_title', title)
        metadata = {
            'confidence': result.get('confidence', 'low'),
            'is_zimbabwean': result.get('is_zimbabwean', False),
            'notes': result.get('notes', ''),
        }
        
        logger.info(
            'Gemini correction: "%s - %s" -> "%s - %s" (confidence: %s, zim: %s)',
            artist, title, corrected_artist, corrected_title,
            metadata['confidence'], metadata['is_zimbabwean']
        )
        
        return corrected_artist, corrected_title, metadata
        
    except json.JSONDecodeError as e:
        logger.warning('Failed to parse Gemini response as JSON: %s', e)
        return artist, title, {'confidence': 'low', 'is_zimbabwean': False, 'notes': 'Parse error'}
    except Exception as e:
        logger.exception('Gemini API error: %s', e)
        return artist, title, {'confidence': 'low', 'is_zimbabwean': False, 'notes': str(e)}


def is_gemini_configured() -> bool:
    """Check if Gemini API is configured."""
    api_key = getattr(settings, 'GEMINI_API_KEY', '')
    return bool(api_key)
