# Voting Studio 2.1 — Windows application

Status: Tauri source implementation. A Windows installer must pass the build and
acceptance checks below before it is distributed. No completed installer is
claimed merely because its workflow is configured.

## What staff install

The client is a Tauri 2 Windows application with a native window, application
menu, taskbar identity and a per-user NSIS installer. Opening the Start Menu
shortcut starts `voting-studio.exe` and displays the station workspace inside its
own WebView2 window. There is no default-browser launch or local development
server to start. Staff machines need no Python, Node.js, Rust or PostgreSQL.

Target: Windows 10/11 x64. The installer embeds Microsoft's offline WebView2
installer, so a missing runtime is handled during installation instead of asking
staff to download it separately. This makes the installer larger. Corporate
device policy can still require administrator involvement. Windows 7, 32-bit
Windows and native ARM64 builds are not acceptance targets for this package.

The central Django service continues to receive votes and run workers. The app
requires connectivity to that service for live data. Installing on another PC
does not create a separate voting database, and closing the app does not stop
the central voting system.

## Everyday use

1. Extract the Windows build artifact and run its `*-setup.exe`.
2. Open **Radio Zimbabwe Voting Studio** from Start.
3. If the server address was not included when building, enter the HTTPS address
   supplied by the station administrator once. Sign in with a staff account.
4. On subsequent launches the saved server opens automatically. If unreachable,
   the bundled connection screen lets you retry or correct the address.
5. Use **Application → Connection settings** to change servers or **Reload
   workspace** after restoring connectivity. Closing the main window exits the
   app; launching a second copy focuses the existing instance.
6. CSV exports save under unique filenames in the user's Downloads folder and
   show a completion message. Export URLs remain authenticated by the webview's
   staff session. Existing files are not overwritten.

Sessions use an incognito webview, so staff should expect to sign in after
restarting the application. Only the server address is written to the app's
per-user settings directory. Provider credentials remain on the server.

## Build once, install on multiple computers

The build machine needs Node.js 22, Rust's MSVC toolchain, Microsoft C++ Build
Tools and the Windows SDK. These are build prerequisites, not staff-PC software.

From PowerShell at the repository root:

```powershell
.\desktop\build.ps1
# Optional: preconfigure the address so staff can proceed directly to sign-in.
.\desktop\build.ps1 -ServerUrl 'https://your-real-voting-server.example'
```

Use your actual deployed HTTPS origin, not the illustrative address above.
Output is `src-tauri/target/release/bundle/nsis/*-setup.exe`. The same setup file
can be copied to the other supported Windows PCs.

For CI, set the optional repository variable `VOTING_STUDIO_SERVER_URL` to the
real HTTPS origin. Run **Quality checks**; the `windows` job runs Rust tests and
builds the NSIS installer with WebView2. The artifact is named
`VotingStudio-Tauri-Windows-x64`, and includes SHA-256 checksums. A successful
artifact upload is required before an installer can be downloaded.

The Tauri and plugin versions and npm lockfile are pinned. The initial Windows
run must generate `src-tauri/Cargo.lock`; it is also uploaded for review. Commit
that generated lockfile and use Cargo's `--locked` flag in release builds before
distributing a production version. Do not fabricate a dependency lockfile.

## Central-server compatibility

Deploy the companion `/api/desktop/status` endpoint before rolling out this
desktop version. It is a public, read-only compatibility response containing only
the application identifier and desktop protocol version. No votes, listeners,
staff information or secrets are returned. It is not a worker-health endpoint.

The native launcher checks this endpoint using TLS verification, a 15-second
timeout, a bounded response body and no automatic redirects. This avoids silently
loading a different site or leaving first launch on an unreachable page. The
server must use a valid HTTPS certificate and its final origin address.

## Security and maintenance

- The bundled setup window alone can read settings or initiate a connection.
  Custom commands are declared in the Tauri application manifest and allowed
  only by a local `setup` capability. Rust also checks the calling window/origin.
- The remote workspace gets no native command, filesystem, shell or opener
  permissions. Main-frame navigation stays on the configured HTTPS origin;
  pop-up windows are denied. The app does not include a browser-opening plugin.
- Settings writes are atomic. No provider tokens or staff passwords are embedded
  in the installer. Preconfigured addresses are public configuration, not secrets.
- Only the authenticated chart-export route can trigger a native download.
- The previous pywebview launcher remains in source for historical compatibility
  and its existing tests, but `desktop/build.ps1` and CI now build Tauri. An old
  pywebview installation is a different installer identity: remove it through
  Windows Settings after validating the new app. No silent destructive migration
  of old software or settings is attempted.
- The installer is currently unsigned. Production distribution needs an
  Authenticode signing certificate or managed signing service to reduce Windows
  trust prompts. Do not tell staff to disable SmartScreen. Even signed software
  remains subject to device policy and reputation checks.

## Validation and release checklist

Local automated checks cover the compatibility endpoint and full Python suite,
JavaScript syntax, the installed Tauri configuration schema, explicit capability
boundaries, installer options and required icon assets. Rust policy tests cover
HTTPS validation, cross-origin navigation, damaged settings and atomic overwrite.
The Playwright desktop test exercises the local UI using a mocked native bridge;
it does not establish native Windows behavior.

Before distribution:

1. Obtain a successful Windows build and run the Rust tests, plus the PostgreSQL
   and browser CI jobs, for the exact commit being distributed.
2. Install on a clean Windows 10 and Windows 11 x64 machine, including a machine
   without WebView2. Confirm runtime installation, shortcuts and uninstall.
3. Check first launch, sign-in, second-instance focus, resizing, CSV download,
   network loss/recovery, invalid certificates, changing servers, exit and relaunch.
4. Verify the remote workspace cannot invoke native setup commands, browse to
   another origin, or launch the system browser.
5. Sign the release, verify its signature and checksum, and test installation
   under the station's actual device policies.

References: [Tauri Windows installers](https://v2.tauri.app/distribute/windows-installer/),
[capabilities](https://v2.tauri.app/security/capabilities/), and
[Windows signing](https://v2.tauri.app/distribute/sign/windows/).
