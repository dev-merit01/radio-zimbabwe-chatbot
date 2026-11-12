# AirVote for Android

This is an installable Android client for the **existing** AirVote Django/Railway staff workspace, not an independent voting service. It renders the same live workspace used by the Windows Tauri software, with responsive navigation, the same station/chart-show permissions, the same AI verification outcomes, archives, CSV exports and central administrator actions. Voting, AI, Bird webhooks and workers continue to run on the server when the app is closed.

## Run or build

- Android 8.0 (API 26) or newer; internet and a trusted HTTPS connection to the running AirVote server.
- Open `mobile/android` in Android Studio with JDK 17, Android SDK platform 35, Gradle 8.9.
- Build the debug APK with `gradle -p mobile/android :app:testDebugUnitTest :app:lintDebug :app:assembleDebug`.
- A GitHub Actions workflow builds the same APK on changes to this directory. Open **Actions → AirVote Android APK**, choose the successful workflow run, and download the `AirVote-Android-debug-APK` artifact.
- The file inside is `app-debug.apk`. Android may warn about a development-signed test build; do not bypass corporate device security policies. A production-signed release needs a secure signing key and device acceptance testing.

The first app launch asks for the **same HTTPS server origin** configured in the Windows client. Use the server URL only, e.g. `https://voting.example.org`, without `/accounts/login/` or other paths. The client validates the public `/api/desktop/status` handshake before opening the site. Staff sign in using their existing AirVote credentials. There are no demo credentials, local vote storage, or API keys packaged in the APK.

## Same features as Windows

| Capability | Source |
|---|---|
| Live station dashboard and incoming votes | Existing Django workspace and its API |
| Chart shows, programmes, chart sizes, previous positions, movement indicators | Existing Django workspace and its API |
| Presenter song eligibility, approval and review queue | Existing server permissions and forms |
| Verified song catalogue and AI-matched incoming votes | Existing Railway/worker pipeline |
| Archives and CSV chart exports | Existing authenticated chart endpoints |
| Station management, presenter accounts, administrator configuration | Existing server-side administrator functions |

The mobile shell adds connection setup, recovery, a native options menu, file choosing and Android DownloadManager. It deliberately has **no separate vote processing**, authentication tokens, native privileged JavaScript bridge or browser-based access to another server origin. Permissions cannot be bypassed by the mobile UI.

## Security and operational notes

- HTTPS is required; the certificate is checked by Android and cannot be ignored.
- The connection is allowed only after the same public compatibility handshake as the desktop app; redirects are not followed during verification.
- The WebView can navigate only inside the configured AirVote origin. User-tapped external HTTPS links open in the Android browser without transferring AirVote cookies.
- WebView file and arbitrary content access are disabled; no camera or microphone permission is granted.
- The native download manager accepts authenticated files only from the configured origin, and forwards cookies only to that origin.
- Change the server using **App options → Connection settings**. On changing origin, the app clears WebView cookies, avoiding session carryover.
- Charts and pending items are **not available offline**. Server-side workers continue to process messages regardless of whether the phone is online.
- Never embed Django, Bird or OpenAI credentials in the Android project.

## Release acceptance tests

1. From a clean installation, reject invalid URLs, non-HTTPS and a foreign HTTPS site; connect to the real server.
2. Sign in as a presenter: check assigned station/show only, eligible songs, live charts, incoming/review; verify admin-only scoring is absent.
3. Sign in as an administrator: create/edit station and shows, manage presenter accounts, approve eligible songs, review spam, publish and export charts; check the same edits appear on Windows.
4. Send a real Bird WhatsApp vote to one of the live station numbers; verify worker status, count and chart movement update in both clients (never submit synthetic test votes to production).
5. Verify CSV downloads in Android notifications; test app restart, log out, token expiry, connection loss, invalid TLS, change of station/server and long tables on a small phone.
6. Before public distribution, set up a private signing key, build a signed release AAB/APK, scan, install and validate on target devices. Debug builds are for testing only.

The debug build and unit tests check app integrity, but do **not** prove that live Bird/OpenAI credentials work or that every mobile device has been tested.
