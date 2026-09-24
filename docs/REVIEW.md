# Version 2 code review

Review date: 24 September 2026. Scope: tracked application modules, routes, templates, provider clients, models/migrations, background work, maintenance commands, dependencies and Windows packaging.

## Issues addressed

- Fixed startup routing and missing dashboard JavaScript.
- Added authenticated, deduplicated receipt and persistent jobs. Receipt does not wait for an external API.
- Separated intake, matching and reply workers. Added queue indexes and bounded lease recovery; uncertain sends are not blindly repeated.
- Replaced whole-station recounts on every review with affected-song reconciliation. Weekly publication reconciles only its date range and refuses unprocessed votes. Removed per-song archive ranking queries.
- Preserved original votes and published chart snapshots. Scoped operations to the assigned station; missing assignments fail closed.
- Retired unsafe legacy mutation engines and commands. Scoped catalogue seeding and Spotify enrichment, preserved review decisions and recorded changes.
- Added JSON shape validation, stricter song editing, same-origin redirects, CSRF checks, login throttling, security headers, no-store staff responses and private desktop sessions. Routed admin login through the throttled entry point.
- Updated dependency pins to tested versions and added CI, including PostgreSQL concurrency and Windows build jobs.
- Added stale-request cancellation, timeouts, clear error/reconnect states and polling that pauses during editing. Unified sign-in and workspace styling, removed unavailable public registration and replaced numeric merge entry with song search.
- Added repeatable development setup, installer definitions and operational documentation.

## Evidence and limits

The local Python suite passes, with the real PostgreSQL locking test skipped on SQLite. Browser checks passed against an isolated fixture database: sign-in, every navigation section, adding/verifying a song, merge search, CSV download, connection loss/recovery and mobile overflow checks. Desktop and mobile screenshots were inspected; the mobile test now disables motion to capture the settled layout. Static analysis and JavaScript syntax checks pass. pip-audit reported no known vulnerabilities in the installed Python environment after updating pip. A source scan for common credential formats found no matches; this does not establish that historical credentials have been revoked.

CI is configured to build the Windows installer and run tests on PostgreSQL. A configured job is not evidence that it has run successfully. Verify the relevant commit's Actions results before distributing an installer.

No live provider credentials or production data were used. Bird subscription payload/signature compatibility, OneMsg's secret-header support, outbound delivery, Spotify and AI integration must be verified with the deployed accounts. Native Windows install/uninstall, WebView2 behaviour, signing and a production-size PostgreSQL migration/load rehearsal remain release gates. No live deployment or webhook switch has been performed.

Django's deployment check still warns about HSTS subdomains and preload. These are intentionally not enabled without knowing the domain's complete HTTPS coverage; they must be decided by the hosting administrator. The core HTTPS, secure-cookie, framing and content-type protections are configured.
