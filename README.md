# AirVote

A centrally hosted voting service with an installable Windows staff application. Telegram and WhatsApp messages enter authenticated, deduplicated webhooks. Durable background jobs record votes, match songs and send replies. The staff workspace provides live weekly results, song review, archives, CSV exports and an audit trail.

## Try everything on one Windows PC

Use the 2.2.0 installer and [same-computer setup instructions](docs/LOCAL_WINDOWS.md).
Install Python 3.12, run `setup-local.cmd` once, then `start-local.cmd`. Open the
installed app and connect to `http://127.0.0.1:8000`. This isolated local mode uses
SQLite and disables live provider integrations; no hosting is needed.

## Windows installation

The desktop client is now **Tauri 2**. It opens the workspace in its own Windows
application window; it does not launch the default browser. The NSIS installer
includes the offline WebView2 installer and installs for the current Windows user.
Staff PCs do not need Python, Rust, Node.js or a local database.

After the Windows build succeeds, download `AirVote-Windows-x64` from
**Quality checks**, extract it and run its `*-setup.exe`. Open **AirVote** from Start, enter the station's HTTPS server once if it is not
preconfigured, then sign in. Change it using **Application → Connection settings**.
Read [Windows installation and release checks](docs/WINDOWS_TAURI.md).

The client contains no provider credentials or local voting database. The central server and workers must be running. Closing the app does not stop voting. Internet access is required.

To build on Windows, install Node.js 22, Rust MSVC and Visual Studio C++ Build
Tools with the Windows SDK, then run:

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

Read [the central-server handover](docs/WINDOWS_V2.md), [Tauri desktop handover](docs/WINDOWS_TAURI.md) and [latest voting review](docs/REVIEW_2026-09-26.md). Production requires PostgreSQL, Redis, HTTPS and supervised intake, matching and reply workers. Rehearse migrations on a backup before changing live services.

```bash
pytest -q
python manage.py check
python manage.py makemigrations --check --dry-run
ruff check --select F,E9 apps radio_zimbabwe desktop
pip-audit -r requirements.txt
```

The CI workflow runs PostgreSQL tests, browser journeys and a Windows installer build. The concurrency test deliberately skips on SQLite. Real provider delivery and interactive Windows installation remain separate acceptance checks.

Legacy `process_votes`, `llm_match`, `clear_database` and polling commands are retired. Use the audited workspace and v2 worker. `load_songs --station radio_zimbabwe` adds missing songs for review without changing existing decisions. `enrich_spotify --station radio_zimbabwe` adds metadata without approving songs or changing vote identity.

## AirVote 2.2.0

Use [versioned releases](https://github.com/dev-merit01/radio-zimbabwe-chatbot/releases) for the installer EXE and separate source ZIP; Actions artifacts expire.
See [release instructions](docs/RELEASE_2.2.0.md) for voting, station passwords and upgrades, and [API setup](docs/API_SETUP.md) for connected mode.

## Station accounts and saved editions

See [station accounts and chart editions](docs/STATIONS_AND_CHARTS.md) for account approval, administrator controls, Saturday Top 20/50 archives and December Top 50 publication.
