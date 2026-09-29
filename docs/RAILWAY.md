# AirVote on Railway

Deployment files are prepared; a connected Railway account, configured secrets and live acceptance are still required. The Windows client remains installed on each PC and connects to the hosted HTTPS server. You do not run the local server or ngrok after cutover.

## Services in one project and region

| Service | Source/config | Purpose |
| --- | --- | --- |
| Postgres | Railway PostgreSQL with persistent volume | Accounts, votes, songs, charts and durable job queue |
| Redis | Railway Redis | Shared cache and request throttling |
| airvote-web | This GitHub repo, main; Dockerfile + service settings | HTTPS workspace and signed webhook intake |
| airvote-worker | Same repo/branch; Dockerfile + worker settings | Ingestion, matching and replies |

Keep the repository root as the build root for both code services. Docker builds Python only, never Tauri/Node. Set explicit service commands as shown below; new Railway services no longer read railway.json/railway.toml. Do not rely on a config-file path. Only web gets a public domain. Keep database connections private and all four services in the same region. Start with one web replica and one worker. Leave service sleeping/serverless OFF: inbound votes and queued work must run when staff close their apps.

## Service deployment settings

Railway deprecated Config as Code for new services on 2026-08-28. Configure these values through Railway service settings/API. Existing legacy config files stop being read on 2026-12-01. For future project-wide automation, import the configured project with `railway config pull`, review `railway config plan`, and preserve secrets rather than exporting their values.

| Setting | Web | Worker |
| --- | --- | --- |
| Builder | Dockerfile, path `Dockerfile` | Dockerfile, path `Dockerfile` |
| Start command | `sh deploy/web.sh` | `sh deploy/worker.sh` |
| Pre-deploy | `python deploy/check_environment.py && python manage.py migrate --noinput` | None |
| Healthcheck | `/healthz/`, timeout 300 seconds | None; check heartbeat in Connections |
| Restart | On failure, 10 retries | On failure, 10 retries |
| Serverless/sleep | Off | Off |

Redis is cache-only; votes and durable jobs are in PostgreSQL. A private Redis 7.4 service can run with a generated password, 64 MB cache ceiling and `noeviction` (fail rather than silently discard rate-limit keys). It needs no public domain. PostgreSQL still needs a persistent volume and backups.

## Variables

Create a unique random secret locally, e.g. `python -c "import secrets; print(secrets.token_urlsafe(64))"`. Save it directly in Railway variables, not Git/chat. Web and worker must share the following values (adjust service names in references if different):

```dotenv
DJANGO_DEBUG=False
DJANGO_SECRET_KEY=<new-random-secret>
DATABASE_URL=${{Postgres.DATABASE_URL}}
REDIS_URL=${{Redis.REDIS_URL}}
DJANGO_ALLOWED_HOSTS=<web-domain>,healthcheck.railway.app
CSRF_TRUSTED_ORIGINS=https://<web-domain>
TRUST_PROXY=True
SECURE_SSL_REDIRECT=True
DJANGO_TIMEZONE=Africa/Harare
LOCAL_WEBHOOK_HOSTS=
AUTO_AI_MATCH=False
WEB_CONCURRENCY=1
WEB_THREADS=4
```

Generate a Railway domain on web, then replace `<web-domain>` with its exact hostname (no scheme in ALLOWED_HOSTS). The healthcheck hostname is required for Railway's deployment probe. `/healthz/` is the only HTTP redirect exception; it returns a generic readiness status after checking database and cache. Staff routes retain HTTPS and authentication. Worker heartbeat/queue status is separate in Connections; a passing web probe does not prove the worker is processing votes.

Add provider variables to BOTH services. For Bird reception: `BIRD_WEBHOOK_SECRET` and `BIRD_STATION`. Sending replies additionally needs `BIRD_ACCESS_KEY`, `BIRD_WORKSPACE_ID`, `BIRD_CHANNEL_ID` for the supported Channels API. Rotate previously shared keys rather than reusing them. Set optional Spotify/AI keys only if enabling those integrations.

Important: current Bird configuration binds one provider account to one station. Two independent station WhatsApp accounts require account-to-station routing work before both can go live on this deployment. Selecting a station in the desktop app does not route inbound provider messages. See API_SETUP.md for supported signatures/payloads and station IDs.

## First deployment and data

1. Back up the existing server database. Decide whether this is a fresh production database or whether existing users, votes and archives must migrate. Do not overwrite or import into production without checking counts and relationships on a staging restore.
2. Create Postgres and Redis, then web with the variables above. The web pre-deploy command validates configuration and runs Django migrations; static files are already in the image. Wait for its readiness check to pass.
3. Start worker with its dedicated settings and the same environment. No Celery or Telegram polling process is needed for this durable-worker/webhook pipeline.
4. In a Railway remote shell connected to web, run `python manage.py createsuperuser` interactively. Do not commit a default admin password. Log in, set station passwords and approve/assign staff accounts.
5. Enable scheduled database-volume backups and verify a restore. Monitor usage and configure billing alerts; the $5 Hobby amount is not a guarantee that four services cost only $5 in total. Do not set an automatic shutdown limit without accepting that votes may be unavailable when it is reached.

## Verify, then cut over

- Confirm login, registration approval, station isolation/passwords, admin-only review and static assets over HTTPS.
- Record a clearly identified test vote, watch the worker process it, verify the song, and confirm chart totals. Retain an audit trail for any test data removal.
- Set Bird's URL to `https://<web-domain>/webhook/bird/` only after backing up and deciding data cutover. Keep signing verification enabled. Send a real WhatsApp test and verify receipt, job processing and correct-station totals. New songs need admin verification to appear on the chart. Re-delivery of the same provider event must not count twice.
- Enter `https://<web-domain>` in the installed AirVote client's server settings. The existing 2.3.0 client can use this hosted address; it does not require a new installer for the hosting move.
- When validated, stop local provider processing/ngrok. Avoid two independently accepting databases during cutover. Preserve the old backup for rollback.

Connect GitHub main to both code services for future deployments. Use Railway's Wait for CI option where available. Schema changes must remain compatible during rolling deployments; a code rollback does not undo database migrations.

References: https://docs.railway.com/guides/django , https://docs.railway.com/config-as-code/reference , https://docs.railway.com/deployments/healthchecks .
