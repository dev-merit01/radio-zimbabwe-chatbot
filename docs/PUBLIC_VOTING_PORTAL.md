# AirVote public online voting portal

## Public address
The new listener website is served at **/vote/** on the same HTTPS host as
the AirVote application. It is separate from /accounts/login/ and all
presenter/administrator routes.

1. Choose a station from the active station directory.
2. Choose a regular/end-of-year chart show. Voting availability comes from
   that show's configuration, not its broadcast airtime.
3. Browse/search the current **published** eligible-song list; do not allow
   free-text artist or programme codes from the browser.
4. Verify a mobile number using a one-time passcode delivered by Bird Verify.
5. Complete Cloudflare Turnstile and submit a selected song.

The server derives station, show, artist, title and code from verified
database records. The web receipt is **queued** in the same InboundEvent,
RawVote, VoteJob and OpenAI verification pipeline as WhatsApp messages.
A queued receipt is *not* an official chart point. Only valid votes matching
a verified, published eligible song are counted.

## Protection and activation

**Public voting is disabled by default.** The homepage and read-only
station/song list can be inspected before activating submissions. Configure
the following **on the Railway web service** before enabling:

| Variable | Value |
| --- | --- |
| `AIRVOTE_PUBLIC_VOTING_ENABLED` | `true` to accept online submissions |
| `AI_ALL_VOTES` | `true` (required; online votes must not bypass AI) |
| `AIRVOTE_TURNSTILE_SITE_KEY` | Public Cloudflare Turnstile site key |
| `AIRVOTE_TURNSTILE_SECRET_KEY` | Private matching Turnstile secret |
| `AIRVOTE_VERIFY_API_KEY` | Private Bird API key with Verify permission |
| `AIRVOTE_BIRD_VERIFY_REGION` | `us1` or `eu1` matching the key; defaults to `us1` |

Create a Turnstile widget for **the exact domain** hosting the public portal.
Do not expose the secret in HTML or JavaScript. Configure Bird Verify's
supported recipient country/channels, available credits, billing and OTP
sending restrictions before enabling. **Phone verification can incur charges.**
No sender number or additional database is needed for Bird Verify, but the
Verify product must be available for your Bird account.

Server controls:
- CSRF on every POST; no browser-provided station/artist text is trusted.
- Cloudflare Turnstile is verified **server-side** for OTP request and each
  vote, including the token's action and hostname.
- Bird generates and validates OTPs; no OTP hashes or raw codes are stored
  in the AirVote vote database.
- A temporary server-side session records the pending phone and then holds
  only its HMAC fingerprint after verification. The fingerprint expires after
  30 minutes. Raw phone numbers are never stored on web vote receipts.
- Shared cache rate limits, duplicate-song and daily vote limits, and
  database transactions are enforced; Redis failures close the voting gate.
- Only enabled open programmes with a presenter-published eligible list
  accept web votes. The public endpoint cannot see pending drafts, staff
  credentials, review queues or internal vote counts.
- Worker processes online events without sending an SMS/WhatsApp reply.
- Keep the server's verification and anti-bot credentials on the web service;
  the existing worker has no need to call these providers.

## Combined voting and source attribution

RawVote now persists an immutable `source` value:
- `web` — Online portal
- `whatsapp` — Bird/OneMsg WhatsApp
- `sms` — reserved for future SMS inbound integration
- `telegram` — Telegram
- `manual` — staff-entered

The migration backfills historical sources from their original listener
channels and does not rewrite chart results. All channels contribute to the
existing show-specific official totals; no separate web chart is created.
Each incoming-vote row shows the source label, and the **Voting channels**
panel separates received, AI-approved, awaiting-AI and needs-review records.
These are *intake metrics*, not equivalent to official chart positions.

Future incoming WhatsApp/SMS votes and web votes with the same normalised
phone share a privacy-preserving HMAC fingerprint for the daily vote limit.
Older stored WhatsApp votes are not retroactively fingerprinted by this
migration; their previous totals and records remain intact. A dedicated
SMS provider/webhook is required before SMS intake can be marked live.

## Deployment checklist

1. Test the portal at /vote/ while the public-voting flag is false.
2. Verify configured Turnstile hostname and both server-side challenge actions.
3. Test Bird Verify send and check using an approved phone and monitor
   verification delivery costs.
4. Enable `AI_ALL_VOTES` on **both** web and worker so every new web
   vote stays pending until verified.
5. Enable the portal flag and cast a test vote on an **open** chart show
   containing a **published** eligible song.
6. Check **Incoming votes → Source**: Online portal; then
   **Voting channels** source breakdown and the correct programme's rankings.
7. Check duplicate voting, closed voting windows, unlisted songs and
   wrong-station IDs remain rejected.
8. If the provider is unavailable or suspicious traffic appears, switch
   `AIRVOTE_PUBLIC_VOTING_ENABLED=false`. Existing staff/WhatsApp intake
   remains operational.

In-flight public web receipts created before disabling are still processed
by the existing worker under ordinary rules. Disabling the portal does not
delete votes or invalidate immutable historical charts.
