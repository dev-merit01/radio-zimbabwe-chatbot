# Station accounts and saved charts

AirVote uses one backend, with independent song records, mappings, reviews, votes
and chart archives for each station. The same artist/title may exist at two
stations with different verification status and totals. Reviewing or merging a
song affects only the selected station. The old SongCatalog/StationSong tables
are retained for migration compatibility and are not used by the voting pipeline
or workspace; no shared song import or shared verification is exposed.

## Accounts and administrators

1. Choose **Create an account** on the sign-in page and select your station.
2. A new account starts inactive. This prevents someone granting themselves access
   to another station simply by choosing it during registration.
3. An administrator opens **Admin control**, checks the requested station, and
   chooses **Approve access**. The member can then sign in.
4. Ordinary accounts can view their station's songs, incoming votes, results and
   archives. Only administrators can add, edit, verify, reject or merge songs and
   save chart editions. Old per-model moderation permissions do not bypass this.
5. **Manage accounts** opens the existing administrator user editor for station
   assignments, password resets and disabling access. The initial admin created by
   setup-local is already an administrator. Administrator means Django superuser;
   the staff checkbox alone does not grant these controls.

Station switching still requires the destination station password, except for
administrators. Rotating a station password revokes existing switch grants.
Personal passwords and station passwords are different. The Windows app already
uses a private WebView and does not retain a signed-in session across app restarts.

## Live charts and Saturday editions

- The weekly voting period is **Sunday through Saturday**, in the server's station
  timezone (Africa/Harare by default).
- On Overview, select **Top 20** or **Top 50**, then **Show chart**. CSV export uses
  the displayed period and size. The catalogue itself remains complete.
- On Saturday, after processing and reviewing votes, the administrator opens
  **Chart archives → Save Saturday chart**, selects that Saturday and Top 20
  (or Top 50). Past Saturdays can also be saved.
- Saving on the broadcast day is permitted. It captures verified totals processed
  at save time, not votes arriving later that evening. Resolve unfinished/failed
  intake and matching jobs before saving. There is no automatic scheduled save.
- An edition is immutable. Its titles, rankings and vote counts do not change if
  songs are later edited, rejected or merged. Repeating the same save returns the
  existing edition; requesting a different size for it is rejected.
- Open an archive by year to revisit or export it. Archives belonging to another
  station cannot be fetched or exported by guessing its ID.

## December Top 50

Choose **Save December Top 50** and a December publication date. This edition
counts verified votes from **January 1 through that date**, and saves the top 50.
Only one year-end edition is saved per station/year. It is separate from weekly
editions, including a Saturday edition on the same December date. If December-only
voting is the desired competition rule, change the annual period policy before
publishing; the current rule is explicitly year-to-date.

## Existing archives and updating

Migration 0013 adds the publication date and separates the weekly/year-end archive
identity. It does not rewrite or delete old snapshots. Existing Monday–Sunday
archives retain their original date ranges and show a legacy week label. If an old
archive occupies the same station/week, it is preserved and cannot be overwritten
with a different period.

Back up the database and stop the running server and worker before updating:

```powershell
cd "C:\Users\Natasha\Desktop\AirVote"
git pull --ff-only origin main
.\setup-local.cmd
.\start-connected.cmd
```

For isolated local use, use `start-local.cmd` instead. `setup-local.cmd` applies
migrations and collects static assets. Existing Windows installations load these
server-side changes after the server update; a new installer is not required.
For hosted deployments run `python manage.py migrate --noinput`, collect static
assets, then restart the web service and worker together.
