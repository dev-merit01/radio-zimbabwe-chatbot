# Radio Zimbabwe Voting Bot

Multi-channel chatbot to collect up to 5 votes per user per day, verified via Spotify, and generate a daily Top 100 chart. Supports both Telegram and WhatsApp (via OneMsg.io).

## Setup

1. Create `.env` from `.env.example` and fill secrets.
2. Install dependencies.
3. Run migrations and start server.

## Commands (Windows PowerShell)

```
python -m venv .venv
. .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

### Local DB (SQLite by default)
The `.env.example` defaults to `DATABASE_URL=sqlite:///db.sqlite3`, which is perfect for local development. No extra setup required.

### Switching to PostgreSQL for deployment
Update your `.env` to point to Postgres and re-run migrations:

```
DATABASE_URL=postgres://<user>:<password>@<host>:5432/<db_name>
REDIS_URL=redis://<host>:6379/0
```

Then:

```
python manage.py migrate
```

## Webhook (Telegram)

### Local Testing
You have two options locally:

**Polling (no public URL required)**

```
python manage.py run_telegram_bot
```

Send a message to your bot and you should receive an immediate reply. Stop the command with `CTRL+C`.

**Webhook with ngrok**

1. Install ngrok: `choco install ngrok` (or download from https://ngrok.com/)
2. Run ngrok: `ngrok http 8000`
3. Copy the HTTPS URL (e.g., `https://abc123.ngrok.io`)
4. Test bot token: `python manage.py test_telegram_bot`
5. Set webhook: `python manage.py set_telegram_webhook https://abc123.ngrok.io/webhook/telegram/`

### Production
Set your bot webhook to `https://<your-host>/webhook/telegram/`.
Incoming messages should include `message.chat.id` and `message.text`.
Use `python manage.py delete_telegram_webhook` to disable it if migrating to polling.

## WhatsApp (OneMsg.io)

### Setup

1. Register at https://app.onemsg.io/
2. Connect your WhatsApp by scanning the QR code in "My Device"
3. Create an application in "My Apps" → "Create Application"
4. Get your API keys: Click "Integration" on your app to find `appkey` and `authkey`
5. Add to your `.env`:
   ```
   ONEMSG_APP_KEY=your-app-key-here
   ONEMSG_AUTH_KEY=your-auth-key-here
   ```

### Webhook Configuration

1. Expose your server publicly (use ngrok for testing: `ngrok http 8000`)
2. In OneMsg dashboard: Go to "My Device" → Select your device → Enter webhook URL:
   ```
   https://<your-host>/webhooks/whatsapp/
   ```
3. All incoming WhatsApp messages will now be forwarded to your bot

### Testing

Send a message to your connected WhatsApp number with a vote:
```
Winky D - Kasong Kejecha
```

The bot will respond with confirmation.

## API

- `GET /api/chart/today` — returns today’s Top 100.

## Celery

Start Celery worker and beat (for daily chart computation):

```
. .venv\Scripts\Activate.ps1
celery -A radio_zimbabwe.celery.app worker -l info
celery -A radio_zimbabwe.celery.app beat -l info
```

You can also call the task manually via Django shell:

```
python manage.py shell -c "from apps.charts.tasks import compute_daily_chart; compute_daily_chart.delay()"
```

## Notes
- Timezone is `Africa/Harare`.
- Spotify search uses top match only.
- Vote limit is 5 per user per day, no edits.
- Logging is configured for console output; adjust `LOG_LEVEL` in `radio_zimbabwe/settings.py` if needed.
- Run `python manage.py test_telegram_bot` to verify credentials whenever delivery issues appear.