"""Evaluate and optionally activate a conservative, station-specific match gate.

Dataset: one JSON object per line: {"input": "...", "canonical": "..."}
where canonical is a VERIFIED station catalogue name, or null for a no-match
example. Include real ambiguous negatives and independent listener variants.
No phone numbers or personal identifiers should enter the dataset.
"""
import hashlib
import itertools
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.voting.calibration import (
    DEFAULTS, Thresholds, secondary_fuzzy_candidate,
)
from apps.voting.catalogue_matching import automatic_candidate, ranked_candidates, normalise
from apps.voting.models import CleanedSong, MatchCalibration
from apps.voting.services import parse_vote_input


def classify(rows, thresholds):
    correct = accepted = positives = wrong = 0
    for expected, ranked, artist, title in rows:
        if expected is not None:
            positives += 1
        selected = (
            automatic_candidate(ranked, artist, title)
            or secondary_fuzzy_candidate(ranked, artist, title, thresholds)
        )
        if selected is None:
            continue
        accepted += 1
        if selected.pk == expected:
            correct += 1
        else:
            wrong += 1
    return {
        "rows": len(rows), "positives": positives, "accepted": accepted,
        "correct": correct, "incorrect": wrong,
        "precision": correct / accepted if accepted else 0.0,
        "recall": correct / positives if positives else 0.0,
    }


def load_cases(path, station):
    catalogue = {
        normalise(row.canonical_name): row.pk
        for row in CleanedSong.objects.filter(
            station=station, status="verified",
        ).only("id", "canonical_name")
    }
    cases = []
    names = set()
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
            message, canonical = item["input"], item["canonical"]
            if not isinstance(message, str) or not (canonical is None or isinstance(canonical, str)):
                raise ValueError("input must be a string and canonical a string or null")
            key = (normalise(message), normalise(canonical) if canonical else None)
            if not key[0]:
                raise ValueError("empty vote")
            if key in names:
                continue  # prevent duplicate examples inflating the benchmark
            names.add(key)
            if canonical is not None and normalise(canonical) not in catalogue:
                raise ValueError("label not in this station's verified catalogue")
        except (ValueError, KeyError, TypeError) as exc:
            raise CommandError(f"Invalid benchmark line {lineno}: {exc}") from exc
        parsed = parse_vote_input(message)
        artist, title = parsed if parsed else (None, message)
        artist = artist or "Unknown Artist"
        expected = catalogue[normalise(canonical)] if canonical is not None else None
        ranked = ranked_candidates(station, artist, title, exhaustive=True)
        cases.append((expected, ranked, artist, title, message))
    return cases


class Command(BaseCommand):
    help = "Calibrate a station's fuzzy match gate from independent positive/negative labels."

    def add_arguments(self, parser):
        parser.add_argument("--station", required=True)
        parser.add_argument("--dataset", required=True)
        parser.add_argument("--activate", action="store_true")

    def handle(self, *args, **options):
        path = Path(options["dataset"])
        if not path.is_file():
            raise CommandError("Labelled dataset not found")
        station = options["station"]
        raw = path.read_bytes()
        cases = load_cases(path, station)
        if not cases:
            raise CommandError("No labelled data")
        train, holdout = [], []
        for expected, ranked, artist, title, message in cases:
            item = (expected, ranked, artist, title)
            # Consistent, nonrandom split by normalised listener expression.
            # A phrase cannot appear in both training and validation.
            bucket = int(hashlib.sha256(normalise(message).encode()).hexdigest()[:8], 16) % 5
            (holdout if bucket == 0 else train).append(item)
        if not train or not holdout:
            raise CommandError("Provide more varied examples for a held-out assessment")

        candidates = [DEFAULTS]
        candidates.extend(
            Thresholds(score, gap, artist, title)
            for score, gap, artist, title in itertools.product(
                (0.86, 0.88, 0.90, 0.92, 0.94),
                (0.08, 0.10, 0.12, 0.15),
                (0.83, 0.86, 0.90),
                (0.83, 0.86, 0.90),
            )
        )
        # Optimise recall subject to NO observed false approvals in training.
        passing = []
        for profile in candidates:
            training = classify(train, profile)
            if training["incorrect"] == 0:
                passing.append((training["correct"], training["precision"], profile))
        selected = max(
            passing, key=lambda item: (item[0], item[1], item[2].min_score),
        )[2] if passing else DEFAULTS
        validation = classify(holdout, selected)
        negatives = sum(case[0] is None for case in cases)
        result = {
            "station": station,
            "labelled_unique": len(cases),
            "negative_labels": negatives,
            "train": classify(train, selected),
            "holdout": validation,
            "selected": vars(selected),
            "sha256": hashlib.sha256(raw).hexdigest(),
        }
        self.stdout.write(json.dumps(result, sort_keys=True))
        if not options["activate"]:
            self.stdout.write("Evaluation only: no live thresholds changed.")
            return

        if not (
            len(cases) >= 500 and negatives >= 100
            and len(holdout) >= 100
            and validation["accepted"] >= 50
            and validation["incorrect"] == 0
            and validation["precision"] >= 0.99
        ):
            raise CommandError(
                "Activation refused: collect >=500 distinct votes, including "
                ">=100 negatives; holdout >=100 with >=50 accepted, >=99% "
                "precision and zero observed false matches."
            )
        MatchCalibration.objects.update_or_create(
            station=station,
            defaults={
                "min_score": selected.min_score, "min_gap": selected.min_gap,
                "min_artist": selected.min_artist, "min_title": selected.min_title,
                "is_active": True, "labelled_count": len(cases),
                "evaluated_precision": validation["precision"],
                "evaluated_recall": validation["recall"],
                "dataset_sha256": result["sha256"],
                "evaluated_at": timezone.now(),
            },
        )
        self.stdout.write("Validation passed; station thresholds activated.")
