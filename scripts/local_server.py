"""Loopback-only local server; --providers opts into explicit provider configuration."""

import argparse
import json
import os
import re
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "http://127.0.0.1:8000"


def local_environment(root=ROOT, providers=False):
    data = root / ".local-voting"
    data.mkdir(exist_ok=True)
    secret = data / "secret.key"
    try:
        with secret.open("x", encoding="utf-8") as output:
            output.write(secrets.token_urlsafe(48))
    except FileExistsError:
        pass
    key = secret.read_text(encoding="utf-8").strip()
    if not key:
        raise RuntimeError("Local secret.key is empty. Restore it from your local backup.")
    env = os.environ.copy()
    env.update({
        "DJANGO_SETTINGS_MODULE": "radio_zimbabwe.settings",
        "DJANGO_SECRET_KEY": key,
        "DJANGO_DEBUG": "True",
        "DJANGO_LOG_LEVEL": "INFO",
        "DJANGO_ALLOWED_HOSTS": "127.0.0.1,localhost",
        "LOCAL_WEBHOOK_HOSTS": "",
        "DJANGO_TIMEZONE": "Africa/Harare",
        "DATABASE_URL": "sqlite:///" + (data / "db.sqlite3").as_posix(),
        "SECURE_SSL_REDIRECT": "False",
        "SECURE_HSTS_SECONDS": "0",
        "TRUST_PROXY": "False",
        "CSRF_TRUSTED_ORIGINS": ADDRESS,
        "AUTO_AI_MATCH": "False",
        "VOTING_DAILY_LIMIT": "5",
        "ALLOW_REPEAT_SONG": "False",
    })
    # Environment values take precedence over the existing project .env file.
    # This local profile must never send real messages or invoke paid providers.
    for name in (
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_WEBHOOK_SECRET", "BIRD_ACCESS_KEY",
        "BIRD_WORKSPACE_ID", "BIRD_CHANNEL_ID", "BIRD_WEBHOOK_SECRET",
        "ONEMSG_APP_KEY", "ONEMSG_AUTH_KEY", "ONEMSG_WEBHOOK_SECRET",
        "SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET", "OPENAI_API_KEY",
        "GEMINI_API_KEY", "COHERE_API_KEY", "ANTHROPIC_API_KEY",
    ):
        env[name] = ""
    if providers:
        import environ
        class ProviderEnv(environ.Env):
            ENVIRON = {}
        path = root / ".env.providers"
        if not path.is_file():
            raise RuntimeError("Create .env.providers from .env.providers.example first.")
        ProviderEnv.read_env(str(path), overwrite=True)
        allowed = {key for key in env if key.startswith(("TELEGRAM_", "BIRD_", "ONEMSG_", "SPOTIFY_", "OPENAI_"))}
        allowed.update({"TELEGRAM_STATION", "BIRD_STATION", "ONEMSG_STATION", "OPENAI_MODEL", "AUTO_AI_MATCH"})
        env.update({key: value for key, value in ProviderEnv.ENVIRON.items() if key in allowed})
        hosts = [host.strip().lower() for host in ProviderEnv.ENVIRON.get("WEBHOOK_PUBLIC_HOSTS", "").split(",") if host.strip()]
        for host in hosts:
            if len(host) > 253 or "." not in host or any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                for label in host.split(".")
            ) or host in {"127.0.0.1", "localhost"}:
                raise RuntimeError("WEBHOOK_PUBLIC_HOSTS must contain exact public hostnames, without https://, paths, ports or wildcards.")
        if hosts:
            env["LOCAL_WEBHOOK_HOSTS"] = ",".join(dict.fromkeys(hosts))
            env["DJANGO_ALLOWED_HOSTS"] += "," + env["LOCAL_WEBHOOK_HOSTS"]
        groups = [("TELEGRAM_BOT_TOKEN", "TELEGRAM_WEBHOOK_SECRET"),
                  ("BIRD_WEBHOOK_SECRET",),
                  ("ONEMSG_APP_KEY", "ONEMSG_AUTH_KEY", "ONEMSG_WEBHOOK_SECRET")]
        if not any(all(env.get(key) for key in group) for group in groups):
            raise RuntimeError("Configure at least one complete messaging provider in .env.providers.")
        for group in groups:
            if any(env.get(key) for key in group) and not all(env.get(key) for key in group):
                raise RuntimeError("Incomplete provider configuration: " + ", ".join(group))
        if any(env.get(key) for key in ("BIRD_ACCESS_KEY", "BIRD_WORKSPACE_ID", "BIRD_CHANNEL_ID")) and not env.get("BIRD_WEBHOOK_SECRET"):
            raise RuntimeError("Bird reception requires BIRD_WEBHOOK_SECRET.")
    return env


def manage(env, *args):
    subprocess.run([sys.executable, str(ROOT / "manage.py"), *args],
                   cwd=ROOT, env=env, check=True)


def setup(env, no_account=False):
    manage(env, "migrate", "--noinput")
    manage(env, "collectstatic", "--noinput")
    manage(env, "check")
    if not no_account:
        # Prompt only on the first setup; never replace existing accounts/passwords.
        manage(env, "shell", "-c", (
            "from django.contrib.auth import get_user_model; "
            "from django.core.management import call_command; "
            "call_command('createsuperuser') if not "
            "get_user_model().objects.filter(is_superuser=True).exists() "
            "else print('Existing local administrator kept.')"
        ))
    print("Local setup complete. Run start-local.cmd, then connect the app to " + ADDRESS)


def start(env, providers=False):
    if not (ROOT / ".local-voting/db.sqlite3").exists():
        raise RuntimeError("Run setup-local.cmd first to create your local database and account.")
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", 8000))
        except OSError as error:
            raise RuntimeError("Port 8000 is in use. Stop the other server before starting this one.") from error
    processes = []
    try:
        web = subprocess.Popen([sys.executable, str(ROOT / "manage.py"), "runserver",
                                "127.0.0.1:8000", "--noreload"], cwd=ROOT, env=env)
        processes.append(web)
        for _ in range(60):
            if web.poll() is not None:
                raise RuntimeError("The local web server stopped. Check the error above.")
            try:
                with urlopen(ADDRESS + "/api/desktop/status", timeout=1) as response:
                    status = json.load(response)
                if status.get("application") == "radio-zimbabwe-voting-studio" and status.get("desktop_api") == 1:
                    break
            except (URLError, TimeoutError, ValueError):
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError("The local server did not become ready.")
        processes.append(subprocess.Popen([
            sys.executable, str(ROOT / "manage.py"), "run_vote_worker", "--allow-sqlite"
        ], cwd=ROOT, env=env))
        print("\nLOCAL SERVER READY: " + ADDRESS, flush=True)
        print("Open the installed AirVote app and enter that address.", flush=True)
        print("Keep this window open. Press Ctrl+C to stop both server and worker.", flush=True)
        print("Connected mode: configured providers may send real replies.\n" if providers else
              "Local test database only; live messaging and AI providers are disabled.\n", flush=True)
        while all(process.poll() is None for process in processes):
            time.sleep(0.5)
        raise RuntimeError("A local server process stopped. Check the error above and restart.")
    except KeyboardInterrupt:
        print("\nStopping local server and worker...")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "start", "check", "account"))
    parser.add_argument("--no-account", action="store_true", help="Skip account prompt for CI setup")
    parser.add_argument("--providers", action="store_true", help="Enable credentials from .env.providers")
    args = parser.parse_args()
    env = local_environment(providers=args.providers)
    if args.command == "setup":
        setup(env, args.no_account)
    elif args.command == "start":
        start(env, providers=args.providers)
    elif args.command == "account":
        manage(env, "createsuperuser")
    else:
        manage(env, "check")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
