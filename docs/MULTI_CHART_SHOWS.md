# AirVote multi-chart-show rollout (seven stations)

AirVote now stores permanent chart-show IDs beneath the existing seven stations:
Radio Zimbabwe, National FM, Power FM, Classic 263, Central Radio, Khulumani FM
and Luzibo Radio.

## Safe compatibility mode

The migration creates **one Historical station chart** and three **disabled,
editable templates** per station:
- `SAT20`: Saturday Top 20 (selectable Top 20 or Top 50)
- `GOS20`: Sunday Gospel Top 20 (selectable Top 20 or Top 50)
- `EOY`: End-of-Year Top 50 (separate annual campaign)

The examples are not activated automatically. Existing raw votes and published
archives are linked to the Historical station chart without changing their
counts, songs or original timestamps.

Each station's **Require programme codes** setting defaults to **off**. In this
mode, code-free messages keep going to the Historical chart; correctly coded
messages can go to enabled programme charts.

## How to activate each station

1. Back up the production PostgreSQL database and confirm the web and worker
   deployments are healthy.
2. Set `BIRD_SILENT_VOTING=true` on **both** Railway web and worker services.
   This suppresses new and previously queued Bird outbound replies; provider
   confirmations sent *to Bird's webhook* still work.
3. Sign in as the central super-administrator and choose the correct station.
4. Open **Chart shows**, edit the template names, voting codes, chart size
   (Top 20 or Top 50), per-listener daily limits and Zimbabwe-time schedule.
   Annual campaigns require explicit opening and closing date/times.
5. Enable the chosen shows. Verify they are accepting votes in their intended
   windows before announcing the codes.
6. Announce the codes to the station's listeners. For example:
   `SAT20 Winky D - Ijipita` or `GOS20 Artist - Song`.
7. Only then enable **Require programme codes** for that station. Unknown
   codes, omitted codes and closed-show submissions are held in an admin-only
   **Unassigned votes** queue, without counting or sending listener replies.
8. Confirm incoming vote, AI processing and live ranking all identify the same
   selected programme. Repeat for the other stations.

**Do not turn on mandatory codes simultaneously for all stations** unless
the broadcasters have announced their codes. Each station can be rolled out
separately.

## Behaviour and safety

- Station from authenticated Bird receiving number; programme from the
  case-insensitive code configured within that station.
- Matching and verification use the existing station's verified song catalogue
  and shared OpenAI API; a vote cannot bypass AI via a programme code.
- Programme votes, listener limits, rankings and published archives are isolated.
- The Historical chart retains the original station-wide tallies. New
  programme votes do not increment those historical tallies.
- Codes and names may change; old votes and archives retain their permanent
  programme IDs. Previously received Bird messages remain mapped to their
  original programme on retry.
- Chart-show templates can be duplicated, disabled or archived. Archiving does
  not delete records; used codes are reserved.
- Manual resolution from **Unassigned votes** requires admin rights and
  a programme that was accepting votes when the message was received.
- A weekly show publishes on its configured broadcast day, using its own
  preceding seven-day voting window. Annual campaigns publish their separate
  December Top 50, based only on campaign-coded votes.
- Published chart editions are immutable, and pending work for that show must
  be resolved before publication.

## Deployment checklist

- `python manage.py makemigrations --check --dry-run`
- `python manage.py check`
- `pytest -q` on PostgreSQL, including browser workflows
- `BIRD_SILENT_VOTING=true` on both services
- Railway web and worker healthchecks both pass
- Test a known song, a typo, an unknown code and overlapping code/schedule
  scenarios for one pilot station
- Enable mandatory codes only after staff have received voting instructions

## Deferred features

Programme-specific gospel/genre eligibility allowlists, polished PDF and XLSX
exports and network-wide AI-cost reporting are optional later enhancements.
Do not mistake the station's shared verified catalogue for a genre-restricted
programme catalogue until an eligibility feature is implemented.
