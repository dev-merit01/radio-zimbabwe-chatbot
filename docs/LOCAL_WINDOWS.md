# Run AirVote entirely on your Windows computer

Use version **2.2.0 or later**. The older 2.1.0 installer accepts HTTPS only.
This local mode opens the installed Tauri window and connects to a Django server
on the same PC. No domain, hosting, Redis or PostgreSQL is needed for this trial.

## First-time setup

1. Download the `codex/tauri-windows` source ZIP and extract it into a new folder,
   for example `C:\VotingStudioLocal`. Do not overwrite your previous project.
2. Install Python **3.12 for Windows x64**, including its **py launcher**. Check
   it in Command Prompt with `py -3.12 --version`. Only the computer hosting the
   local server needs Python; the installed desktop app does not.
3. Double-click `setup-local.cmd` inside the extracted project. Internet access
   is needed for the first dependency installation. Keep its window open.
4. When prompted, choose an administrator username, email and password. Password
   characters are not displayed while typing. Remember these details: there is
   no default account/password. Re-running setup preserves an existing account.
5. Download/extract the matching Windows installer artifact and run
   `AirVote_2.2.0_x64-setup.exe`. It includes WebView2. The
   installer remains unsigned and subject to Windows/device policy checks.
6. Double-click `start-local.cmd`. Wait for **LOCAL SERVER READY** and leave that
   console window open. It starts both the web server and one voting worker.
7. Open **AirVote** from Start. Enter exactly:

   ```text
   http://127.0.0.1:8000
   ```

8. Select **Open AirVote** and sign in with the account created in step 4.
   The workspace opens in the app's own window. No browser is launched.

If an old remote address is saved, use **Application → Connection settings**.

## Daily start and stop

- Start: double-click `start-local.cmd`, then open the installed app.
- Stop: close the app, then press **Ctrl+C** in the server console. This stops
  the server and worker. Closing the app alone does not stop the server.
- Do not run multiple copies of the local server. If port 8000 is occupied,
  stop the existing server before trying again.

The local account, songs and votes persist under `.local-voting` in the extracted
project folder. This folder contains the SQLite database and local secret key.
Do not delete it to restart the app. Back it up with the server stopped. Moving
only the installed app does not move the server's data.

## What this trial can and cannot do

Use it for sign-in, the staff workspace, catalogue/review workflows, charts and
CSV export using local test records. A new database is empty; no fake listener
votes or shared passwords are automatically added.

The local script deliberately uses a separate SQLite database and ignores live
provider credentials even if your existing `.env` contains them. It does not
edit that `.env`, your previous database or production settings. Telegram,
WhatsApp, external AI and Spotify integrations are disabled in this profile.
Public providers cannot deliver webhooks to `127.0.0.1`; end-to-end live voting
requires a later explicitly configured integration environment.

The HTTP exception is restricted to the numeric address `127.0.0.1`. Remote
servers still require HTTPS. Do not bind this development server to `0.0.0.0`
or expose it on your network. Each PC running this local setup has its own test
database. Later, configure staff apps to use one shared HTTPS server.

## Troubleshooting

- **`py` not recognized / no Python 3.12:** install Python 3.12 with its Windows
  launcher, open a new terminal, and verify `py -3.12 --version`.
- **Setup failed:** retain the console error and fix it before running Start.
  Re-run setup after correcting it; do not delete your data folder.
- **Cannot connect:** leave `start-local.cmd` running, check its readiness
  message, and enter the exact HTTP address above. Verify the installed app is
  version 2.2.0 or later.
- **Need another administrator:** in a terminal at the project root, run
  `.venv-local\Scripts\python.exe scripts\local_server.py account`.
- **Server check:** run
  `.venv-local\Scripts\python.exe scripts\local_server.py check`.

For deployment and the central-server architecture, see `WINDOWS_TAURI.md`.

## Voting and API mode

Use **Incoming votes → Record vote** to submit a manual vote. Keep the worker
running; review new songs before they enter the verified chart. To opt into
provider credentials, use [API_SETUP.md](API_SETUP.md) and start-connected.cmd.
The default start-local.cmd remains isolated. See [release notes](RELEASE_2.2.0.md)
for station passwords and administrator account management.
