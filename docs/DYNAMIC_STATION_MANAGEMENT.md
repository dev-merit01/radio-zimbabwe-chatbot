# AirVote reusable multi-station management

AirVote now has an authoritative **BroadcastStation** database directory.
The original seven station IDs are seeded unchanged. Admin users can add
further stations without deploying another software version, editing the
Django station enum or rebuilding the Windows desktop app.

## Central administrator onboarding

1. Sign in with the superadministrator login and choose **Manage stations**
   in the sidebar or under **Admin control → Network configuration**.
2. Click **Add station**, enter a display name, and optionally its Bird
   WhatsApp receiving number and HTTPS logo. AirVote generates a permanent,
   unique internal station ID. A newly created station is always **disabled**.
3. Click **Open workspace** for that station. Its **Chart shows** workspace
   starts with a historical view only; add as many programmes as required.
   Each programme has a unique station-local, editable vote code, Top
   10/20/40/50 size, scheduled/manual/annual voting period and daily limits.
4. In **Presenter accounts**, create or assign presenter logins to their
   correct station and individual chart show. The central administrator
   remains unrestricted.
5. In **Eligible songs**, the presenter selects verified songs and publishes
   the edition directly before voting begins; no administrator approval is required.
6. Connect the receiving WhatsApp number **in Bird's portal** and ensure the
   signed webhook points to AirVote. The phone number added in AirVote
   configures incoming routing only — it cannot buy numbers or connect
   WhatsApp automatically.
7. Open **Manage stations → Configure station** and activate it. Enable the
   desired chart shows after setting their voting windows and announce
   the programme codes. **Require programme codes** is ON by default
   for *new* stations so code-free messages cannot count in a wrong chart.
   The original stations retain their previous policy.
8. Verify a real Bird message using a programme code, its station, chart,
   matched song and AI status. Review unassigned or suspicious messages.

## Existing station configuration

Existing stations are seeded with their original, unchanged station slugs,
show IDs, votes, users, historical archives and Bird environment mapping.
Their existing display names and logos are preserved. The central admin
may update a display name or logo URL without changing its permanent ID.

Existing Bird numbers in `BIRD_NUMBER_STATION_MAP` remain a compatible
fallback. Database-configured numbers take precedence if there is no
conflict. Admin station settings prevent assigning a number to a
different station where Railway already reserves it. To retire one of
the original environment-based numbers entirely, an operator must
also remove the corresponding legacy Railway route. Do not assume
clearing a database field disables an environment-defined number.

## Invariants

- Station ID is immutable and never recycled. No station-delete action.
- New station starts disabled; deactivating an established station requires
  disabling its active chart shows first. Accounts and past votes stay.
- One database-configured Bird receiving number per station; network-wide
  uniqueness enforced. Additional incoming numbers require future
  extension rather than bypassing the station mapping.
- An unrecognised or inactive Bird destination cannot be used to inject
  votes into another station. The station name in a listener's payload
  is never accepted as the routing authority.
- New stations require voting codes; programme ranking remains dependent
  on station-approved eligible songs after that programme's first lock.
- Shared OpenAI matching is station-scoped; no new API key required for
  each station. Listener replies remain suppressed when silent voting is
  enabled on both web and worker services.
- All station/profile, presenter, programme, chart and vote queries retain
  their existing station isolation. Admin station creation and updates are
  audited in ReviewAudit.
- The product UI, registration form, staff station switch, branding, Bird
  intake, voting service and operational catalogue tasks use the database
  station list rather than a fixed list of seven.

## Deployment checks

- `python manage.py makemigrations --check --dry-run`
- `python manage.py migrate --noinput`
- `pytest -q` with PostgreSQL and signed Bird webhook tests
- Browser onboarding for a newly created station
- Railway web and worker health checks
- Pilot activation and one real inbound vote before broadcasting new codes

Note: External Bird provisioning and WhatsApp number verification always
remain separate provider actions. The software does not send paid replies.
