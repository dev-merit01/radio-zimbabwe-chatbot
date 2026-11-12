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
4. Enter an email address and confirm the six-digit one-time verification code
   delivered by AirVote's secure SMTP provider. **No phone number is requested.**
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
| `AIRVOTE_EMAIL_HOST` | SMTP host for verified transactional emails |
| `AIRVOTE_EMAIL_PORT` | SMTP port (default `587` for STARTTLS) |
| `AIRVOTE_EMAIL_USERNAME` | SMTP username |
| `AIRVOTE_EMAIL_PASSWORD` | Private SMTP password / provider API key |
| `AIRVOTE_EMAIL_FROM` | Verified sender address, e.g. `votes@yourdomain` |
| `AIRVOTE_EMAIL_USE_TLS` | `true` (default, STARTTLS) |
| `AIRVOTE_EMAIL_USE_SSL` | `false` (set `true` for port 465; disable TLS then) |

Create a Turnstile widget for **the exact domain** hosting the public portal.
Do not expose the secret in HTML or JavaScript. Configure a reliable email
provider (SMTP credentials and a verified sending domain) before enabling the
public portal. Email delivery may incur provider fees or quota limits. The
existing Bird WhatsApp integration is not used to verify website listeners.

Server controls:
- CSRF on every POST; no browser-provided station/artist text is trusted.
- Cloudflare Turnstile is verified **server-side** for OTP request and each
  vote, including the token's action and hostname.
- AirVote creates a cryptographically random six-digit code and sends it via
  the configured SMTP service. The pending session stores only a keyed digest
  of that code; it expires after 10 minutes and has strict attempt limits.
- The pending session temporarily contains the email address for delivery.
  After verification, it is replaced by a keyed HMAC email identifier with a
  30-minute expiry. Neither the address nor the code is stored on web vote
  receipts or published to presenters.
- Common Gmail dot and +label aliases are normalised to the same identity.
  This helps prevent repeat voting under aliases, but no email-only system
  can guarantee that a person cannot create multiple distinct mailboxes.
- Shared cache rate limits, duplicate-song and daily vote limits, and
  database transactions are enforced; Redis failures close the voting gate.
- Only enabled open programmes with a presenter-published eligible list
  accept web votes. The public endpoint cannot see pending drafts, staff
  credentials, review queues or internal vote counts.
- Worker processes online events without sending an SMS/WhatsApp reply.
- Keep the server's verification and anti-bot credentials on the web service;
  the existing worker has no need to call these providers.

## Understanding "Voting closed" versus missing songs

The public portal shows distinct, **independent** reasons a vote cannot be
accepted. A programme's normal voting hours are **not** the same as public
web-voting activation:

- **Awaiting song list:** no eligible edition has been published for the
  currently selected chart show and date. Existing verified songs in the
  station-wide catalogue are not automatically on that chart. The presenter
  signs in with station credentials and chooses the programme, then opens
  **Eligible songs**, creates the next edition, adds the verified songs, and
  selects **Publish eligible list** before the voting window opens.
- **No eligible songs:** a published edition exists but it currently has no
  eligible verified songs.
- **Voting hours closed:** the chart show's own admin-configured weekly,
  manual or campaign schedule does not allow submissions at this time.
- **Online voting not active:** email delivery, Cloudflare Turnstile or the
  public feature flag is not yet configured. The chart's voting hours may
  still be open, and WhatsApp intake is unaffected.
- **Voting open:** all the checks above pass. A listener can select a
  published song, verify an email and submit a vote for AI processing.

The blue **Browse eligible songs** panel is a real button: tapping it scrolls
to the searchable song list. If the presenter's list is missing, tapping
explains why there is no song to select. Email/vote controls are hidden
until votes can actually be submitted, so the portal never suggests a
disabled button is functional. The backend continues to reject closed,
unpublished and unverified submissions regardless of UI state.

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

New online votes enforce limits using an HMAC fingerprint of the **verified
email address**. WhatsApp/SMS use phone-based identities and remain unaffected.
Without a common identifier, the system cannot reliably deduplicate a person
across email-only online votes and phone-based WhatsApp/SMS votes. Do not claim
such cross-channel protection. Existing chart totals remain unchanged.
A dedicated SMS webhook is required before SMS intake can be marked live.

## Deployment checklist

1. Test the portal at /vote/ while the public-voting flag is false.
2. Verify configured Turnstile hostname and both server-side challenge actions.
3. Test secure email delivery, correct-code confirmation, wrong codes,
   expiry and the resend limits. Monitor SMTP provider costs and quotas.
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
