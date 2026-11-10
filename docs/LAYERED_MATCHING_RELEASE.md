# AirVote indexed + semantic + calibrated verification

This release adds the three previously missing components while keeping vote
approval AI-first and preserving all verified song tallies and published charts.

## 1. PostgreSQL indexed retrieval (default enabled)

Migration `0024_semantic_and_indexed_matching` attempts to enable the standard
`pg_trgm` extension and GIN indexes over the verified catalogue title,
artist, canonical name and approved alias text. Queries remain station scoped.

On managed databases without extension privileges, migration intentionally
falls back to existing complete-catalogue matching. In that case inspect the
Railway migration warning; do not assume an index was installed.

`AI_INDEXED_SEARCH_ENABLED=true` enables a bounded SQL token/trigram
prefilter. If no strong candidates are found, a complete scan preserves recall.
Before any non-exact OpenAI-confirmed vote is counted, an exhaustive station
ambiguity check protects against competitors missed by the index.

## 2. OpenAI semantic embeddings (opt-in, metered)

No external vector DB is necessary yet. `SongEmbedding` stores a precomputed
normalised OpenAI vector and SHA-256 fingerprint of the canonical song text in
PostgreSQL. At the current catalogue size, exact cosine top-K search is
sufficient. This is real vector retrieval, but not an approximate ANN index.
It should be load-tested again when catalogues become much larger.

Run for each station after migration:

```sh
python manage.py refresh_song_embeddings --station radio_zimbabwe --dry-run
python manage.py refresh_song_embeddings --station radio_zimbabwe --limit 1000
```

Only changed or missing records are embedded; requests are batched (max 64).
OpenAI `text-embedding-3-small` is the default model, separately configurable.
Do not log keys or expose embeddings to the desktop application.

Environment flags:

- `AI_VECTOR_ENABLED=false` (default, no query embedding charges)
- `AI_VECTOR_AUTO_REFRESH=false` (default, no startup backfill charges)
- `OPENAI_EMBEDDING_MODEL=text-embedding-3-small`
- `AI_VECTOR_RETRIEVAL_MIN_SIMILARITY=0.64`

Once the corpus is available and costs are approved, set
`AI_VECTOR_ENABLED=true`. The matching worker only calls the embeddings API
when an indexed/lexical score is weak. Query vectors are cached briefly to
avoid paying repeatedly for the same normalized phrase. Set
`AI_VECTOR_AUTO_REFRESH=true` only if a bounded, idempotent catalogue refresh
is desired at worker startup. This flag should be enabled on the matching
worker service only, not on unrelated web service replicas.

Vector-only candidates are included in the model shortlist, but NEVER count
votes solely because of embedding similarity. OpenAI plus a verified,
unambiguous lexical/alias identity is still required. Different song versions
and stations are isolated.

## 3. Calibrated acceptance thresholds (requires real labels)

`MatchCalibration` stores the threshold profile, label count, data hash,
held-out precision and recall for each station. Default active settings stay
conservative until independent labelled data supports an update.

Create a JSON Lines file of real, independently reviewed submissions, using
`{"input": "Hulingende Bhura", "canonical": "Hulengende ... - Bhura Dhanzi"}`.
Use `null` for genuinely invalid/ambiguous no-match cases. Never include
phone numbers or provider event IDs. Run:

```sh
python manage.py calibrate_vote_matching --station radio_zimbabwe --dataset labelled-votes.jsonl
```

The command compares candidate/threshold performance on a deterministic
held-out sample and reports train/validation precision and recall without
altering production. Only when the labelled dataset contains at least 500
distinct submissions, including 100 negatives, with at least 100 held-out
cases, at least 50 accepted held-out matches and zero observed false approvals
with >=99% precision may the command be rerun with `--activate`.

This does not prove a 99% real-world accuracy rate: a held-out test is only
as representative as its labels. Validate per station and monitor a changing
distribution of incoming votes. Turn off a problematic profile by setting
`is_active=false` in the database; defaults automatically resume.

The optional per-station fuzzy gate is used ONLY after a successful, bounded
OpenAI verification. Review decisions, published editions and the original
raw votes remain authoritative.

## Rollback / safe activation

- Disable semantic query requests with `AI_VECTOR_ENABLED=false`.
- Disable the indexed SQL prefilter with `AI_INDEXED_SEARCH_ENABLED=false`.
- Disable per-station tuned thresholds with `AI_CALIBRATED_MATCHING_ENABLED=false`.
- Never delete or reset original raw votes or chart snapshots.
- Do not promise a specific review rate without representative tests.
