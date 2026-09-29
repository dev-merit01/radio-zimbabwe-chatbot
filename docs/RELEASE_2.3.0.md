AirVote 2.3.0 preview

- Redesigned studio welcome and sign-up screens. The form scrolls independently on smaller displays; the whole page stays fixed.
- Refined admin dashboard with network metrics, station cards, account approvals and station-specific publication/operations controls.
- Consistent title-case artist/song display, with DJ, MC and R&B preserved. Original listener messages remain intact; song IDs, vote totals and stored historical snapshots are unchanged.
- No Refresh buttons. Pages refresh automatically when the user is not editing.
- Selecting a station immediately switches an administrator. Station team members receive a password prompt, with cancellation returning to the previous station. No Go button.
- Completed white A with cyan waves on a blue icon background, included in desktop, taskbar and installer assets.
- Includes personal profiles, explicit ngrok webhook hosts, station-specific music, account approval and Saturday/December chart archives.

Update the server: back up the database, stop the server/worker, run `git pull --ff-only origin main`, `setup-local.cmd`, and `start-connected.cmd` (or `start-local.cmd` for isolated testing).

Close AirVote and run `AirVote_2.3.0_x64-setup.exe` to update the native desktop/taskbar icon. Updating source alone does not change the installed EXE. If a pinned icon stays old, unpin it, start the updated AirVote from Start, and pin the running application again.

The installer is for the Windows client. `AirVote-2.3.0-source.zip` contains the server and setup scripts. Checksums accompany both. This is an unsigned preview; live Bird delivery still requires correct webhook credentials and signature verification.

Icon source: imagegen edit of the original AirVote mark. Prompt: preserve the white A and cyan waves, complete the lower-right foot, balance both legs, and use a deep royal blue square background with clear padding. Packaged assets: `static/images/airvote/app-icon.png`, `desktop/ui/assets/app-icon.png`, `src-tauri/icons/128x128.png`, and `src-tauri/icons/icon.ico`.
