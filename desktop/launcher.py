"""Windows client for the centrally hosted Voting Studio. No provider secrets."""
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit


def validate_url(value):
    if not isinstance(value, str) or any(ord(c) < 32 for c in value):
        raise ValueError("Enter a valid HTTPS server address.")
    value = value.strip()
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Enter the HTTPS address supplied by your station administrator.')
    if parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise ValueError('Use the server origin only, for example https://voting.example.org.')
    _ = parsed.port
    return value.rstrip('/') + '/'


def configuration_path():
    return Path(os.environ.get('LOCALAPPDATA', str(Path.home()))) / 'RadioZimbabweVotingStudio' / 'config.json'


def choose_server(path):
    import tkinter as tk
    from tkinter import simpledialog, messagebox
    root = tk.Tk()
    root.withdraw()
    try:
        while True:
            value = simpledialog.askstring('Voting Studio setup', 'Station server HTTPS address:', parent=root)
            if value is None:
                return None
            try:
                url = validate_url(value)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({'server_url':url}), encoding='utf-8')
                return url
            except (ValueError, OSError) as exc:
                messagebox.showerror('Setup could not be saved', str(exc), parent=root)
    finally:
        root.destroy()


def main():
    path = configuration_path()
    url = None
    if '--configure' not in sys.argv:
        try:
            url = validate_url(json.loads(path.read_text(encoding='utf-8'))['server_url'])
        except (OSError, ValueError, KeyError, TypeError):
            pass
    if not url:
        url = choose_server(path)
    if not url:
        return
    try:
        import webview
        webview.settings['ALLOW_DOWNLOADS'] = True
        webview.create_window('Radio Zimbabwe · Voting Studio', url=url,
                              width=1360, height=900, min_size=(900,600),
                              background_color='#F5F7F3', text_select=True)
        # No Python API bridge is exposed to remote page content.
        webview.start(gui='edgechromium', debug=False, private_mode=True)
    except Exception:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror('Voting Studio could not start',
            'Check that Microsoft Edge WebView2 Runtime is installed. '
            'You can also open your station server address in a browser.', parent=root)
        root.destroy()


if __name__ == '__main__':
    main()
