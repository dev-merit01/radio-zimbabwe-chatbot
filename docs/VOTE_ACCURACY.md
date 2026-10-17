# Vote accuracy update

This update screens provider messages before creating InboundEvent rows. Media,
links, number-only or symbol-only input, control characters and overlength text
are acknowledged with HTTP 200 and ignored. This prevents provider retries.
Rejected messages do not create reply jobs, so listeners do not receive a
WhatsApp/Telegram rejection reply. Help/start commands still receive replies.
Manual entry uses the same gate and returns a visible validation error.

Valid-looking but unknown text remains subject to review; syntax validation is
not a claim that a song exists. Numeric titles are allowed when an artist is
present. Unknown title-only submissions remain pending. Common Unicode dashes
and a pipe separator are accepted. Spaced separators preserve hyphenated artist
names. Recording labels such as Live, Remix and Acoustic are retained.

## Matching

The live worker now streams the station's verified catalogue and retains at
most eight ranked candidates. It considers both artist and title similarities,
so artist misspellings are no longer excluded before AI matching. Existing
reviewer mappings remain authoritative. New catalogue decisions are audited.

Unique normalised exact matches are accepted. Non-exact automatic matches need
artist and title similarities of at least 0.90, a combined score of at least
0.94, a runner-up gap of at least 0.08 and titles of at least six characters.
These thresholds are conservative heuristics, not calibrated probabilities.
They should be evaluated against a representative labelled station dataset.

Optional AI receives only candidates with both similarities at least 0.75.
Only a high-confidence result selecting the leading candidate with the required
gap can be applied. Short titles and differing recording-version labels cannot
be auto-resolved by AI. Other decisions stay pending. AI errors leave the durable
job available for retry. AI decisions are stored in LLMDecisionLog. Provider keys
and AUTO_AI_MATCH configuration are unchanged.

Candidate retrieval uses bounded memory but scans the station catalogue for each
new unmatched spelling. Benchmark and introduce indexed candidate retrieval if
catalogues or unique-message traffic grow substantially.

## Counting

When repeat songs are disabled, live counts subtract excess submissions for the
same listener identity, day and mapped song. Both incremental processing and
rebuilds apply the same rule, including after an administrator merges aliases.
RawVote, RawSongTally and published snapshots remain unchanged for auditing.
Raw submissions still consume daily submission slots; excluded duplicates do
not restore slots. Channel identities remain separate, as before.

Historical imported tallies without individual RawVote records cannot be
retrospectively deduplicated. Only duplicates evidenced by retained raw records
are deducted. Existing mappings are not reinterpreted automatically, including
old mappings that collapsed recording variants before this update.

## Deployment

No schema migration is added. Deploy matching web and worker code together,
then recount each affected station using its existing repeat-vote policy:

```sh
python manage.py reconcile_votes --station radio_zimbabwe
```

The command preserves raw submissions and published charts and records an audit
entry. Repeat it for other stations as needed. This update does not replace the
catalogue or introduce the proposed 400-song dataset.

Local validation uses SQLite. The repository CI PostgreSQL suite remains the
gate for production concurrency and SQL behaviour; real provider delivery and
Windows installer checks remain separate acceptance checks.
