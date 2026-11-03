# AirVote automatic Windows updates

AirVote 2.4.0 introduces native Tauri updater checks. The installed Windows application
checks for a newer signed release on launch and offers **Install update** or **Later**.
Staff can also select **Help → Check for updates…**. Installation closes the Windows
client, but the hosted voting system and vote worker continue processing votes.

Only *desktop* code changes require a new signed Windows release. Django/JavaScript
workspace changes published to Railway continue to appear after refresh without updating
the EXE. Existing 2.3.0 installations do not contain the updater and must be upgraded
to the first signed 2.4.0 installer once.

## One-time repository signing setup (repository owner)

**Do not paste the private key or its password into a chat, issue, commit, or website.**
Generate the key pair locally on a trusted Windows build machine, within an AirVote
checkout with Node.js and `npm ci` completed:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.tauri" | Out-Null
npx tauri signer generate -w "$env:USERPROFILE\.tauri\airvote.key"
```

The command creates `airvote.key` (private) and `airvote.key.pub` (public). Save
an offline backup of the private key and its password. Losing them will prevent
updates to users with this public key embedded in their installed client.

In GitHub repository **Settings → Secrets and variables → Actions**:

1. Under **Secrets**, create `TAURI_SIGNING_PRIVATE_KEY` with the *full content*
   of `airvote.key` (not the public key). If prompted for a password when
   generating it, also create `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` with that
   password; otherwise leave the password secret unset.
2. Under **Variables**, create `AIRVOTE_UPDATER_PUBLIC_KEY` with the complete
   content of `airvote.key.pub`. The public key is safe to share.
3. Open **Actions → Quality checks → Run workflow**, choose **main**, and run it.
   Both backend and Windows tests must pass before a stable release is published.

The Windows builder injects the public key **at compile time**, signs the Windows
NSIS installer with the private key, and verifies that a matching `.sig` file exists.
The release job publishes the installer, signature, SHA-256 checksum, source archive,
and `latest.json` to GitHub Releases. AirVote checks
`https://github.com/dev-merit01/radio-zimbabwe-chatbot/releases/latest/download/latest.json`
over HTTPS and Tauri verifies the installer signature **before** running it.
The hosted web workspace has **no updater/plugin permissions**.

If signing secrets are absent, CI may generate an unsigned test installer **only as
an artifact**. Such a build does not enable secure updates and cannot publish a
stable auto-update release. This is intentional.

## Releasing later desktop versions

1. Bump the version together in `src-tauri/Cargo.toml` and
   `src-tauri/tauri.conf.json`, following semantic versioning.
2. Commit the new desktop changes and the refreshed `src-tauri/Cargo.lock`.
3. Push to `main`. GitHub Actions builds, tests and publishes the *new version*.
   If its version already exists in Releases, publishing stops until it is bumped.
4. AirVote detects the newer version the next time it starts. The user must
   approve installation; it is not installed silently without consent.

No OpenAI/Bird token or Railway credential is needed in the installer. Do not
rotate the updater signing key casually: installations only trust the embedded
public key. Rotate through an explicitly planned transition if necessary.

## Troubleshooting

- **No update notice:** Confirm the installed desktop build is at least signed 2.4.0,
  the GitHub release is stable (not a prerelease), the manifest version is higher,
  and internet access to GitHub Releases works.
- **Cannot check:** Check network access or inspect the `latest.json` asset in the
  newest GitHub release.
- **Signature validation fails:** Do not bypass verification. Confirm the public
  key was compiled from `airvote.key.pub` and the manifest signature matches the
  exact installer.
- **Windows installer closes AirVote:** Expected for a signed Windows update.
  Reopen AirVote afterwards if it does not relaunch automatically.
