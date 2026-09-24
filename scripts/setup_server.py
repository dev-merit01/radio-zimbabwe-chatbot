"""Initialise a local/staging server; never silently overwrites existing secrets."""

import secrets
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if not env_file.exists():
    env_file.write_text(
        "DJANGO_SECRET_KEY="
        + secrets.token_urlsafe(48)
        + "\nDJANGO_DEBUG=True\nDJANGO_ALLOWED_HOSTS=localhost,127.0.0.1\nDATABASE_URL=sqlite:///db.sqlite3\n",
        encoding="utf-8",
    )
for command in [("migrate",), ("collectstatic", "--noinput"), ("check",)]:
    subprocess.run([sys.executable, "manage.py", *command], cwd=root, check=True)
print("Server initialised. Create an operator with: python manage.py createsuperuser")
print(
    "For local development: python manage.py runserver and, in a second terminal, python manage.py run_vote_worker --allow-sqlite"
)
print(
    "Production requires HTTPS, PostgreSQL, Redis and separate intake/matching/replies workers. See docs/WINDOWS_V2.md."
)
