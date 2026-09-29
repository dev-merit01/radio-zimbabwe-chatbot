"""Consistent music labels without changing raw listener messages or match keys."""
import re

_ACRONYMS = {"dj": "DJ", "mc": "MC", "r&b": "R&B", "ii": "II", "iii": "III", "iv": "IV"}


def music_name(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"[^\W\d_]+(?:['’][^\W\d_]+)*", lambda m: m[0][0].upper() + m[0][1:].lower(), text)
    return re.sub(r"\b(?:dj|mc|r&b|ii|iii|iv)\b", lambda m: _ACRONYMS[m[0].lower()], text, flags=re.I)
