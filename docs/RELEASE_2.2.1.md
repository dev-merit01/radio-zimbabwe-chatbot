AirVote 2.2.1 preview

- Transparent A-and-waves Windows icon, without the square background.
- One small logo at the top of login and connection screens.
- Includes Bird receive-only support without a channel ID and the current whatsapp.received message format. Signature verification remains required. Automatic replies through the new Bird API are not implemented.

Update the server with git pull --ff-only origin main, then run setup-local.cmd and restart start-connected.cmd (or start-local.cmd for isolated mode). Back up .local-voting before updating.

Close AirVote and run AirVote_2.2.1_x64-setup.exe to update the installed desktop/taskbar icon. Pulling source alone cannot update the installed EXE. If Windows retains an old pinned icon, unpin it, launch the updated AirVote from Start, and pin the running app again.

AirVote-2.2.1-source.zip contains the server and setup scripts; the EXE installs the desktop client. Checksums accompany both files. This remains an unsigned preview.
