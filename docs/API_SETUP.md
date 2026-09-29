# AirVote API and voting setup

## Before connecting providers

Start and sign into AirVote. In **Incoming votes → Record vote**, submit an actual
manual vote using a stable listener reference and `Artist - Song`. Keep the worker
running. **Recent submissions** shows the processing state and result (including
quota/duplicate rejection). Received totals update after intake; a new song enters
Review queue. Verify it to include its votes in the chart. Already verified songs
are matched automatically. Dashboard polling is every 15 seconds. `queued` is not proof a vote has been accepted.

Manual votes are an audited source with the same per-listener/day rules. Listener
identity is scoped to source and station; a manual reference is not automatically
linked to a Telegram account or WhatsApp phone. Do not use manual entry to replay
votes already received from a provider. This is real local data, not a demo button.

## Same-PC integration mode

1. Stop start-local.cmd with Ctrl+C.
2. Copy `.env.providers.example` to `.env.providers` in the server folder.
3. Fill the required fields for your provider, including its station ID. For
   Bird reception, only BIRD_WEBHOOK_SECRET is required; leave BIRD_CHANNEL_ID
   blank for receive-only mode. Other providers still require their full group. Supported station IDs: `radio_zimbabwe`, `national_fm`,
   `power_fm`, `classic_263`, `central_radio`, `khulumani_fm`.
4. Run start-connected.cmd. It uses the same local database and worker, but opts
   into the configured provider credentials. It may send real replies when
   authenticated provider events arrive. No sends are performed just by startup.
5. Check **Connections** for configured providers, worker status and queue errors.
   "Configured" verifies presence only, not provider account validity.

The connected profile still binds only to 127.0.0.1 and is a development server.
Do not expose it wholesale to the internet. For a supervised integration trial,
use an HTTPS ingress forwarding only the required `/webhook/.../` routes, with
provider authentication intact. Never expose debug pages or staff/admin routes.
For deployment use the production PostgreSQL/Redis/HTTPS configuration with
DEBUG=False, proper host/proxy settings and supervised workers.

## Supported webhook contracts

| Provider | Path | Authentication | Vote event |
| --- | --- | --- | --- |
| Telegram | `/webhook/telegram/` | `X-Telegram-Bot-Api-Secret-Token` equals TELEGRAM_WEBHOOK_SECRET | New private `message`, numeric update_id and chat.id |
| Bird WhatsApp | `/webhook/bird/` | Standard Webhooks HMAC headers with a base64/whsec_ key | whatsapp.received with data.from.phone_number and data.text.body; legacy incoming payloads also supported |
| OneMsg WhatsApp | `/webhook/whatsapp/` | `X-Webhook-Token` equals ONEMSG_WEBHOOK_SECRET | id, sender and payload.conversation / extendedTextMessage |

Bind each provider's station in server configuration; a station value in an
incoming payload cannot change that binding. This release supports one configured
account/channel per provider per server. Multiple simultaneous station accounts
need explicit account-to-station routing; changing a staff station does not
redirect provider traffic.

For Telegram, register your public HTTPS webhook with setWebhook and the same
secret_token. Do not run polling and webhooks for the same bot simultaneously.
The project command `python manage.py set_telegram_webhook HTTPS_URL` uses the
normal server environment, not `.env.providers`; use it only with corresponding
server settings loaded. Do not put your bot token into screenshots or chat.

For Bird, select inbound events and the Standard Webhooks signing contract for
your account. Legacy Bird subscriptions with another signature format are not
interchangeable. For OneMsg, your provider or a trusted validating gateway must
attach the secret header; arbitrary unauthenticated webhook delivery is rejected.

Send one real test message through each configured account. Confirm: authenticated
receipt → inbound done → Incoming votes/received total → matching done → review
(if needed) → verified chart → provider reply. Redeliver the same provider message
ID and confirm the count does not increase. Inspect uncertain replies against
provider records; do not blindly retry unknown delivery outcomes.

Automated tests cover the supported payloads, signatures, deduplication, station
isolation and chart pipeline, with mocked outbound sends. They cannot prove your
provider account, credentials, network or actual webhook configuration works.

Primary references:
- https://core.telegram.org/bots/api#setwebhook
- https://bird.com/en-ca/docs/guides/webhooks

## Bird without channel IDs

Current Bird WhatsApp reception uses the `whatsapp.received` event,
`data.from.phone_number`, `data.text.body`, and `data.whatsapp_id`. Subscribe
to that event at `/webhook/bird/`. Standard Webhooks signature verification
remains required. A 403 is an authentication failure, not a missing channel ID.

`start-connected.cmd` accepts a signing secret without a channel ID, workspace
ID or API access key. `BIRD_STATION` selects the destination station. Connections
shows reception separately from legacy replies. Without all three legacy outbound
credentials, new inbound events are processed without creating automatic reply
jobs. Existing queued replies are not removed. This patch does not implement
outbound sending through Bird's new API.

Stop the server, run `git pull --ff-only origin main`, and restart
`start-connected.cmd`. No database migration or desktop reinstall is needed.
For a 403, check the exact endpoint signing secret, restart after editing it,
and inspect the names of the signature headers in ngrok. Never disable signature
verification. The startup script loads the local Django secret automatically;
starting manage.py directly does not load the same local profile.

Reference: https://bird.com/docs/guides/whatsapp/webhooks/messages
