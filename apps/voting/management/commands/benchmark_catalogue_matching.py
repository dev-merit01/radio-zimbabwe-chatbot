"""Read-only candidate retrieval benchmark for labelled listener votes.

Example:
    python manage.py benchmark_catalogue_matching --station radio_zimbabwe \
        --dataset apps/voting/fixtures/catalogue_benchmark.jsonl

Add one JSON object per line with input and canonical (verified catalogue
canonical_name). No listener phone numbers or personally identifying data.
This is NOT an end-to-end LLM accuracy measurement.
"""
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from apps.voting.catalogue_matching import automatic_candidate, ranked_candidates
from apps.voting.models import CleanedSong
from apps.voting.services import parse_vote_input


class Command(BaseCommand):
    help = "Measure station-scoped candidate recall and lexical auto-gate accuracy."

    def add_arguments(self, parser):
        parser.add_argument("--station", required=True)
        parser.add_argument("--dataset", required=True)

    def handle(self, *args, **options):
        station = options["station"]
        path = Path(options["dataset"])
        if not path.is_file():
            raise CommandError(f"Benchmark dataset not found: {path}")
        verified = {
            song.canonical_name.casefold(): song.pk
            for song in CleanedSong.objects.filter(
                station=station, status="verified"
            ).only("id", "canonical_name")
        }
        counts = {
            "rows": 0, "label_missing_in_catalogue": 0, "evaluated": 0,
            "correct_in_top_k": 0, "lexical_gate_correct": 0,
            "lexical_gate_false_positive": 0, "lexical_gate_review": 0,
        }
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                raw = row["input"]
                label = row["canonical"]
                if not isinstance(raw, str) or not (label is None or isinstance(label, str)):
                    raise ValueError("Invalid input/canonical")
            except (ValueError, KeyError, TypeError) as exc:
                raise CommandError(f"Invalid benchmark line {lineno}: {exc}") from exc
            counts["rows"] += 1
            expected = verified.get(label.casefold()) if label is not None else None
            if label is not None and expected is None:
                counts["label_missing_in_catalogue"] += 1
                continue
            parsed = parse_vote_input(raw)
            artist, title = parsed if parsed else (None, raw)
            artist = artist or "Unknown Artist"
            candidates = ranked_candidates(station, artist, title)
            auto = automatic_candidate(candidates, artist, title)
            counts["evaluated"] += 1
            if expected is not None and expected in [item[3].pk for item in candidates]:
                counts["correct_in_top_k"] += 1
            if auto and auto.pk == expected:
                counts["lexical_gate_correct"] += 1
            elif auto:
                counts["lexical_gate_false_positive"] += 1
            else:
                counts["lexical_gate_review"] += 1
        self.stdout.write(json.dumps(counts, sort_keys=True))
        self.stdout.write(
            "These metrics evaluate candidate retrieval only, not live OpenAI "
            "verification, duplicate handling or chart eligibility."
        )
