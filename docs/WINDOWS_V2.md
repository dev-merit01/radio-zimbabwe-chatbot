# Voting Studio 2.0: implementation and handover

Status: development implementation, not a production release.

## Architecture

The Windows client opens the centrally hosted Django workspace using pywebview and Edge WebView2. PostgreSQL, provider credentials, intake, matching and outgoing replies remain on the server. Closing the desktop client does not stop a running central server and worker. Internet access is required. The client stores only the configured HTTPS server address, uses private browsing sessions, and exposes no Python methods to the remote page.

The workspace provides a weekly chart (15-second refresh), incoming votes, paginated song review and catalogue, audited editing/verification/rejection/merging, completed-week archive publishing, CSV exports and provider configuration/worker status. Configuration status is not a live provider health check. Read-only users cannot mutate songs; reviewers and publishers require Django permissions.

## Windows build

Requirements: Windows x64, Python 3.12 x64, Inno Setup 6 and Microsoft Edge WebView2 Runtime. From the repository root in PowerShell:

```powershell
.\desktop\build.ps1
```

Build output: `dist/installer/VotingStudio-2.0.0-Setup.exe`.
The installer creates Start Menu entries for the app and server configuration. First launch asks for the central HTTPS origin. Sign in with an account created by the administrator. Credentials for Telegram, WhatsApp, Spotify or OpenAI never belong in the installer. The installer is not code-signed. WebView2 must be installed separately if absent.

The build script and installer definition have been prepared on Linux, not executed on Windows. Do not describe a Windows executable as available until this build and a Windows smoke test succeed. pywebview API reference: https://pywebview.flowrl.com/api/ and renderer documentation: https://pywebview.flowrl.com/guide/web_engine .

## Central server staging

Back up the existing database first and rehearse on a restored staging copy. Use PostgreSQL for production. Configure `.env` from `.env.example` with a unique secret, host names, Redis and provider settings. Keep DEBUG false in production. Do not switch live webhooks until staging acceptance is complete.

```bash
python -m pip install -r requirements.txt
python manage.py migrate
python manage.py collectstatic --noinput
python manage.py createsuperuser
python manage.py check --deploy
```

Run the web service and each worker under a restart-capable process manager. Separate queues prevent a slow provider or AI call from holding up intake:

```bash
gunicorn radio_zimbabwe.wsgi:application --bind 127.0.0.1:8000 --workers 2
python manage.py run_vote_worker --queue intake
python manage.py run_vote_worker --queue matching
python manage.py run_vote_worker --queue replies
```

Place the web service behind HTTPS. Enable TRUST_PROXY only when a trusted reverse proxy strips client-supplied forwarding headers. Provider webhooks must reach the public HTTPS server; staff pages require authentication. Provision station profiles and only the required review/add/publish permissions. Registration is disabled.

The worker uses durable database jobs, leases and bounded retries. Replies with ambiguous send outcomes are marked uncertain rather than resent automatically. Investigate those against provider logs. Run only one worker for SQLite development; it is not a concurrency validation environment.

## Verification completed

- 99 Python tests passed; the PostgreSQL-only concurrency test is skipped locally and included in CI.
- Django system checks passed; no model migrations are missing.
- JavaScript syntax check and browser journeys passed: login, seven sections, add/verify, merge search, CSV download, offline/reconnect and mobile layout.
- Installed Python dependencies passed pip-audit with no known vulnerabilities reported. This is a dependency advisory check, not a guarantee of security.
- Tests cover authenticated/deduplicated receipt, durable ingestion, review reconciliation, immutable archive snapshots, station boundaries, CSRF, access permissions and desktop URL validation.

## Remaining release gates

- Build and install on Windows; exercise login, resizing, CSV download, disconnect/reconnect, server reconfiguration and uninstall. Browser flows have been checked with Chromium; native Windows WebView2 still needs its own acceptance test.
- Exercise concurrent votes and multiple workers against PostgreSQL, and rehearse migration on a production database copy.
- Verify Bird webhook signature format against the exact configured Bird product/subscription before use. The current handler assumes Standard Webhooks headers/signing; do not assume compatibility. OneMsg requires a provider or gateway that supplies the configured secret header.
- Test actual inbound messages and outbound replies for each enabled channel, and real AI/Spotify access. Credentials and live provider accounts were not used in these tests.
- Rotate previously exposed credentials and revoke them at their providers; deleting a tracked file does not revoke its historical contents.
- Review disabled legacy admin mutations and management commands before operational use. Use the audited workspace for review and publishing.
- Confirm intended business rules: daily limit defaults to five, repeated songs disabled, AI matching disabled until explicitly configured. Limits are per channel identity, not a verified cross-channel person.

Keep the original system running until these gates pass. No production deployment, database migration or webhook switch was performed.
