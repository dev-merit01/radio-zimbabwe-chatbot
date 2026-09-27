AirVote 2.2.0 is a preview release for station acceptance testing.

Downloads are different:
- **AirVote_2.2.0_x64-setup.exe** installs the Windows app (offline WebView2 included).
- **AirVote-2.2.0-source.zip** contains the local server, setup-local.cmd and start-local.cmd.
- Matching `.sha256` files verify each download. GitHub Release assets are not Actions artifacts and have no Actions retention expiry; they remain available while the release/repository remains public and the owner retains them.

First use: extract the source ZIP, install Python 3.12 with its Windows launcher, run setup-local.cmd and create an administrator account. Install the EXE, run start-local.cmd, launch **AirVote** from Start and connect to http://127.0.0.1:8000.

Record a vote: choose **Incoming votes → Record vote**. Enter a consistent listener reference and `Artist - Song`. The worker processes it, and Recent submissions shows acceptance or rejection. Received totals refresh automatically; new songs need **Review queue → Verify** before contributing to the verified chart.

Station passwords: an administrator selects a station and uses **Connections → Set station password**. Other users must enter that station's password when switching. Password rotation revokes switched sessions. Administrators (superusers) bypass the prompt. Manage staff profiles and permissions through **Connections → Manage staff**. Without a configured station password, non-admin switching into that station is denied.

API mode: copy `.env.providers.example` to `.env.providers`, configure your chosen provider on the server and start **start-connected.cmd** instead of start-local.cmd. Do not run both. Tokens never belong in the installer. Real inbound webhooks require an authenticated, publicly reachable HTTPS endpoint; localhost alone is not reachable by Telegram/WhatsApp. See docs/API_SETUP.md for supported providers and limitations.

Upgrade from 2.1.x: stop the app/server and back up `.local-voting` before updating. Keep the existing server folder/data (or copy the stopped `.local-voting` folder into the new source folder), run setup-local.cmd again to apply migrations, then install AirVote. The earlier prototype may retain its old shortcut; uninstall that prototype after validating AirVote. Application identifier and protocol ID are retained for compatibility; station names, including Radio Zimbabwe, remain station names, not the app brand.

Validation gates include PostgreSQL tests, browser voting and station-password journeys, Rust policies, Windows silent installation, native startup, local sign-in page loading and single-instance behavior. Live provider sends use test doubles; real account credentials and delivery have not been verified.

The Windows installer is unsigned. This preview is not a claim of production certification; clean Windows 10/11 and station policy testing, signing, production hosting and live provider acceptance remain necessary.
