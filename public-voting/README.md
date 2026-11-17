# AirVote public voting portal — production integration guide

The public portal is a **separate Cloudflare Worker and D1 holding database**.
It is **disabled** unless configured. It never connects to AirVote's PostgreSQL
database and never exposes staff-only chart controls. The existing WhatsApp and
SMS services continue to operate independently.

## What listeners see

- Station selector with AirVote's own configured station logo and name.
- An existing voting code, or a direct presenter-issued signed voting link.
- Only eligible, administrator-approved songs for that programme's active
  voting period. Search matches artist and title immediately without a lookup.
- Google sign-in + Cloudflare Turnstile + a secure vote receipt.
- No publicly browsable chart-show directory, ranking, chart archive or
  current vote tally.

## Configuration remains in AirVote

AirVote remains authoritative for broadcaster and station ownership; station
status; chart-show codes, schedules, opening and closing hours, limits and
presenter shortlists; daily station-wide limits; repeat-song policy; and
verified song status. These are queried afresh when a listener opens the
ballot. AirVote's private worker also rechecks them when admitting a vote.

Use AirVote desktop's chart-show **Copy online voting link** control to share
an exact programme link after setting `AIRVOTE_PUBLIC_VOTING_URL`.
A code can also be entered from the public station selector; no programme
list is made public.

## Provision the Cloudflare side (not done automatically)

1. Set up a Cloudflare account with phishing-resistant MFA and least-privilege
   operator roles. Do not reuse your main Railway or Google credentials.
2. Create a D1 database named `airvote-public-votes`. Replace
   `REPLACE_WITH_D1_DATABASE_ID` in `wrangler.jsonc` with its ID.
3. Initialise the holding schema from `public-voting`:
   `npx wrangler d1 execute airvote-public-votes --remote --file=schema.sql`
4. Create a production **Turnstile** widget scoped to the portal hostname;
   record its public site key and private secret. Set action `vote`.
5. Create a Google Identity Services Web OAuth client ID with the **exact**
   authorised JavaScript origin of the public portal and compliant Google
   consent-screen settings. No Google client secret is required by this flow.
6. Set the following **public vars** in `wrangler.jsonc`:
   - `BACKEND_URL`: authoritative AirVote HTTPS URL
   - `GOOGLE_CLIENT_ID`: Google public Web client ID
   - `TURNSTILE_SITE_KEY`: Turnstile public site key
7. Use `npx wrangler secret put NAME` for these **private secrets**,
   all distinct, generated with a cryptographically secure RNG:
   - `BRIDGE_READ_SECRET` (at least 32 random bytes encoded as text)
   - `PULL_SECRET` (different, at least 32 random bytes)
   - `VOTER_HASH_KEY` (different, stable per environment; changing it resets
     web account vote fingerprints)
   - `TURNSTILE_SECRET`
8. Run `npm run check && npm test` and deploy the Worker using
   `npx wrangler deploy`. Register the portal hostname and enforce TLS.
9. In the **existing Railway AirVote web service**, set:
   - `AIRVOTE_PUBLIC_VOTING_URL` to the portal HTTPS origin
   - `AIRVOTE_WEB_BRIDGE_SECRET` to exactly the Cloudflare
     `BRIDGE_READ_SECRET`
   Keep the write/pull credential out of the public web service where
   possible, by placing it only in the worker service.
10. In the **AirVote Railway intake worker**, set:
   - `AIRVOTE_WEB_HOLDING_URL` to the same portal HTTPS origin
   - `AIRVOTE_WEB_PULL_SECRET` to exactly the Cloudflare `PULL_SECRET`
   The worker only makes signed outbound requests to Cloudflare's claim
   and acknowledgement endpoints.
11. Restart the worker with the current code. Before enabling public voting,
   validate a complete sample receipt and its final verified tally.

**Never** put any of these four secrets in the frontend, Git, logs, pull
requests or AirVote desktop settings. Never allow the browser to use the
private claim/ack endpoints.

## Guarantees and limitations

- One Google account is identifiable by its stable `sub` claim. The gateway
  stores only a station-scoped HMAC of that claim. Raw Google ID tokens,
  names, plaintext emails and IP addresses are not stored in D1.
- Turnstile tokens are verified server-side for the expected hostname and
  action. The Google token is verified for signature, issuer, audience, expiry
  and verified email status. Google account ownership is **not** proof of one
  unique human or one phone number.
- Gateway D1 `pending → processing → accepted/rejected` receipts use private
  claim leases. AirVote PostgreSQL has a durable UUID ledger so claiming or
  acknowledging twice does not duplicate a tally. Final approval still goes
  through AirVote's existing AI/reviewer vote-processing pipeline.
- Changes to station settings are applied centrally. Already received votes
  are rechecked against the exact song, eligibility and original receipt time.
  Voting days follow the AirVote `Africa/Harare` timezone by default.
- Completed D1 receipts are removed after 24 hours; unresolved receipts are
  rejected after 72 hours and retained briefly for status checking. D1 is not
  intended as a permanent archive or sole system of record.
- Cloudflare Workers/D1 free-tier limits apply; the service must **fail
  closed** on exhausted quotas rather than skip the holding layer or
  verification. Do not promise that a national-scale audience will fit a
  free tier without measured load tests.
- Public web Google accounts and WhatsApp phone numbers are not linked.
  Cross-channel limits cannot be guaranteed for one person without optional,
  consent-based account linking.
- Cloudflare and Railway are both high-trust operational boundaries. Use
  distinct credentials, MFA, scoped tokens, monitoring, backup procedures,
  secret rotation and incident runbooks.

## Release acceptance checks

- Verify eligibility changes, closed and manual-open/closed shows, archived
  shows, licence suspension, and company/station isolation.
- Test concurrent votes at the same limit and the same request ID submitted
  repeatedly. Confirm only one canonical RawVote.
- Test receipt-time voting hours, timezone midnight, retry/lease expiry,
  network failures and AirVote worker restarts.
- Confirm Google token tampering, different OAuth audience, expiry,
  Turnstile replay, invalid receipt secret, and missing bridge signatures
  all fail closed.
- Test mobile breakpoints, keyboard-only navigation, assistive technology,
  station-logo failures, high/low latency and automatic search filtering.
- Run an independent security review and a penetration test before a
  high-stakes national broadcasting launch.

## Music catalogue

Portal ballots search the **existing, approved AirVote programme song list**.
Public visitors cannot create official songs or bypass station eligibility.

AirVote already has station-scoped verified music and Spotify lookup logic.
For Zimbabwean music, build coverage through broadcaster-reviewed uploads,
licensed catalogue partners and artist submissions, not by scraping Spotify.
Apple's iTunes Search API offers limited optional search enrichment; its
promotional assets and previews are subject to Apple usage terms, and search
results do **not** imply eligibility. MusicBrainz's public web API is free
for non-commercial use; commercial integration requires the appropriate
licence. No external search provider can be treated as a complete inventory
of Zimbabwean songs.
