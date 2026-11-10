# AirVote weekly and annual chart-position movement

The AirVote dashboard, presenter shortlist and published chart archives
now distinguish **ranking movement** from vote volume.

## Chart calculation policy

- Position #1 means the highest valid vote total in that programme's
  current chart period. A station multiplier affects the separately labelled
  point total equally for all songs, never their authentic vote count.
- **Previous position** is taken from the **immediately preceding FINALIZED
  edition** of the same permanent chart-show ID within the same station:
  last week's broadcast for weekly programmes, last year's Top 50 for
  annual programmes. Other shows, stations, unfinished charts and different
  chart types are excluded.
- **Green ↑1:** current #1, previous #2 (climbed one position).
- **Red ↓3:** current #4, previous #1 (dropped three positions).
- **Grey → SAME:** previous and current position are equal.
- **NEW:** absent from the preceding published programme chart and has never
  appeared in that programme's earlier official editions.
- **RE-ENTRY:** absent from the immediately preceding published chart but
  did appear on an earlier edition of the same programme.
- **— / No previous chart:** no directly preceding official publication is
  available, including for a newly launched show or a missing week.
  This is deliberately not labelled NEW or RE-ENTRY to avoid making claims
  that the historical data cannot support.

All movement arrows have textual descriptions and contrast colours, so
colour-blind presenters and viewers are not dependent on colour alone.

## Workflow

1. After the previous weekly edition is published, open the ongoing
   chart-show dashboard. The **Last week** column shows #2, #1 etc;
   **Movement** shows green ↑, red ↓, → SAME, NEW or RE-ENTRY.
   Until the current show is officially published, these positions may
   change as valid votes arrive.
2. Open **Eligible songs** while preparing the next edition. Each selected
   candidate shows its **last published position and previous verified
   vote count** where available. This is reference information, not
   an editorial ranking or an automatic eligibility decision.
3. The central administrator approves and locks the shortlist; genuine
   verified votes determine positions without manual movement overrides.
4. When publishing, AirVote snapshots each song's position, previous
   position, movement status, weeks on chart, peak position and the ID
   of the exact prior edition used. Those saved values do not change when
   votes or shortlists subsequently change.
5. Open **Chart archives → View chart** to see the archived movement,
   or **Export CSV** to download rank, title, artist, verified votes,
   optional weighted points, previous position, signed change and
   movement status. A previous position of #2 and change +1 must never
   be silently rewritten.

## Historical archives

Older published editions predate the movement-status field. Their
previous_rank and vote records remain unchanged. For those archives
only, the API derives the best available movement status from the
previous published programme edition and earlier programme history.
For newly published editions movement statuses are frozen, including
the distinction between a genuine NEW entry, RE-ENTRY and unavailable
comparison.

## Data isolation and safety

- No mixing of Saturday vs Gospel vs annual programmes or different
  stations. Names and editable voting codes may change without changing
  a chart show's permanent database identity.
- No comparison over a skipped/unpublished week. If there is no official
  preceding chart, show unavailable, not a fabricated previous ranking.
- Song identification remains through the station's own verified
  catalogue. Ineligible nominations do not enter official rankings.
- This is visualisation, historical metadata and CSV enrichment only.
  Bird webhook delivery, OpenAI billing, voting limits, spam rules,
  legitimate listener votes and locked results are not changed.
