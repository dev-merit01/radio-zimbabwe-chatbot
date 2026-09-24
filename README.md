# Radio Zimbabwe Voting Studio 2

A centrally hosted voting service with an installable Windows staff application. Telegram and WhatsApp messages enter authenticated, deduplicated webhooks. Durable background jobs record votes, match songs and send replies. The staff workspace provides live weekly results, song review, archives, CSV exports and an audit trail.

## Windows installation

Download `VotingStudio-Windows-Installer` from a successful **Quality checks** GitHub Actions run, extract the ZIP and run `VotingStudio-2.0.0-Setup.exe`. A successful build is required before this download exists. Install Microsoft Edge WebView2 Runtime if it is absent. On first launch, enter the HTTPS server address supplied by your station administrator, then sign in. Use the Start Menu **Configure station server** shortcut to change the address.

The client contains no provider credentials or local voting database. The central server and workers must be running. Closing the app does not stop voting. Internet access is required.

To build on Windows yourself, install Python 3.12 x64 and Inno Setup 6, then run:

```powershell
.\desktop\build.ps1
```

## Local development

Use Python 3.12. These commands initialise a new development database, not an existing production installation:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
python scripts/setup_server.py
python manage.py createsuperuser
python manage.py runserver
```

In a second terminal with the same environment:

```bash
python manage.py run_vote_worker --allow-sqlite
```

Sign in at http://127.0.0.1:8000. No demonstration passwords or listener data are installed by this setup. Public registration is disabled. Staff accounts require a station profile; reviewers need `voting.change_cleanedsong`, catalogue contributors need `voting.add_cleanedsong`, and chart publishers need `voting.add_weeklychart`.

## Production and validation

Read [the deployment and Windows handover](docs/WINDOWS_V2.md) and [the review report](docs/REVIEW.md). Production requires PostgreSQL, Redis, HTTPS and supervised intake, matching and reply workers. Rehearse migrations on a backup before changing live services.

```bash
pytest -q
python manage.py check
python manage.py makemigrations --check --dry-run
ruff check --select F,E9 apps radio_zimbabwe desktop
pip-audit -r requirements.txt
```

The CI workflow runs PostgreSQL tests, browser journeys and a Windows installer build. The concurrency test deliberately skips on SQLite. Real provider delivery and interactive Windows installation remain separate acceptance checks.

Legacy `process_votes`, `llm_match`, `clear_database` and polling commands are retired. Use the audited workspace and v2 worker. `load_songs --station radio_zimbabwe` adds missing songs for review without changing existing decisions. `enrich_spotify --station radio_zimbabwe` adds metadata without approving songs or changing vote identity.
