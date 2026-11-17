# AirVote commercial licensing — operator guide

Developer and licensor: Kuiper Data Labs | https://www.kuiperdatalabs.co.zw

## Implementation and safety

AirVote uses one Ed25519-signed licence per station and installation UUID. A signed licence specifies
the customer, plan, station slug, start date, expiry and grace period.
The private signing key must remain encrypted and offline under Kuiper control.
Railway receives only the base64 PUBLIC verification key.
Workspaces and administration are restricted when the feature is enabled and
a station has no valid licence. Incoming webhook votes and background workers
are not interrupted, preserving listener votes while licensing is resolved.

## Create a signing key on a private Kuiper computer

Install project requirements, then:

    python scripts/airvote_licence_issuer.py create-keys --directory ./private-issuer

Back up the encrypted private key and its password separately; never share,
commit or upload them. The generated airvote-public.txt contains the base64
PUBLIC verification key. Copy that text to AIRVOTE_LICENCE_PUBLIC_KEY on
Railway only after reviewing the deployment and access controls.

## Issue a signed annual station licence

    python scripts/airvote_licence_issuer.py issue --key ./private-issuer/airvote-private.pem --installation-id YOUR-INSTALLATION-UUID --station national_fm --customer 'EXAMPLE Broadcaster' --plan annual --start 2026-11-18 --expires 2027-11-17 --grace-days 3 --output example.lic

Check the signed customer Order Form before issuing a real file. The issuing
script also supports trial and enterprise plans. Multi-station agreements
require a separate signed licence per station; the token is not transferable.

## Safe first rollout

1. Require passing CI, legal approval and a tested PostgreSQL backup.
2. Resolve any unrelated staged changes in Railway before deployment.
3. Deploy reviewed code with AIRVOTE_LICENSING_ENFORCED=false.
4. Apply Django migrations and configure AIRVOTE_LICENCE_PUBLIC_KEY (PUBLIC only).
5. Install each station token using the authenticated /licensing/activate/
   screen, or run: python manage.py install_airvote_licence /path/station.lic
6. Confirm /api/licensing/status for each station shows permitted=true.
7. Test expiry, grace, revocation, chart APIs and uninterrupted webhook intake
   on staging. Enable AIRVOTE_LICENSING_ENFORCED=true in production only
   when all active customer stations have valid grants.
8. To roll back enforcement, turn the flag off. Preserve all vote and licence data.

## Operations

To renew, issue and install a new signed token. Never edit signed expiry
claims in PostgreSQL. To revoke locally, use:

    python manage.py revoke_airvote_licence national_fm --reason 'Contract ended'

The system refuses reinstallation of licences issued at or before revocation.
Valid states: active, grace, expired, revoked, unlicensed, not_started,
invalid and misconfigured. The station certificate alone does not activate
AirVote: only the signed AV1 token activates it.

## Current limitations

This first station-based release does NOT enforce individual Windows machine
installation limits, named-seat counts, online self-service purchases,
automated billing, email renewal reminders or remote central revocation.
These must be built and tested before being advertised or put in contracts.

Customer contracts must use the End User Licence Agreement, commercial Order
Form, Data Processing Addendum and Support Schedule, subject to legal review.

## Optional non-disruptive licence enforcement for existing stations

Each customer deployment sets AIRVOTE_LICENCE_INSTALLATION_ID to a persistent
unique UUID and AIRVOTE_LICENCE_PUBLIC_KEY to the vendor PUBLIC verification key.
AIRVOTE_BOOTSTRAP_LICENCES is an optional JSON map of existing station slugs to
signed transitional AV1 tokens. The map is only for stations already in use;
new stations must receive a new signed licence and be activated. A local
database licence overrides the transitional map, including a revocation.
Do not include the private signing key or password in this configuration.
Before enabling enforcement, confirm every active station is represented by a
valid signed licence and has enough time to renew before expiry.
