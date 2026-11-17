# AirVote company-wide licensing | Kuiper Data Labs

**Commercial rule: ONE licence per broadcasting company, NOT per radio station.**

A broadcasting organisation holds a single vendor-signed AV2 commercial licence
that covers all stations explicitly assigned to that organisation. For example,
a broadcaster operating five radio stations needs ONE company licence. Each
station still has its own chart shows, presenter roles, vote-routing codes and
anti-spam policy. No per-station activation, purchase or renewal is required.

Official developer and licensor: https://www.kuiperdatalabs.co.zw

## Model

- BroadcastCompany: legal customer / broadcasting group.
- CompanyStation: one-to-many station ownership; a station belongs to only
  one company. Only the AirVote administrator can change this association.
- CompanyLicence: at most one current company licence. Cryptographically
  signed AV2 token identifies company, installation, licence ID, plan, customer
  and validity dates. The signing private key NEVER enters Railway.
- AV1 StationLicence records are retained for history and safe legacy rollback.
  They do not control access once company licensing is enabled.
- Company status is checked server-side for all protected staff API, dashboard
  and administration requests; vote intake and processing are preserved.

## Existing AirVote transition

The migration places existing stations in one transitional company
slug: existing-airvote. The name is deliberately provisional until the
customer's legal name is confirmed. New stations explicitly belong to a
company and inherit its valid licence automatically.

Old station enforcement remains active during migration by default.
Only after a valid AV2 company licence has been installed and tested may
AIRVOTE_COMPANY_LICENSING_ENFORCED=true be set. It takes precedence over the
legacy AIRVOTE_LICENSING_ENFORCED flag; do not accidentally re-enable AV1
enforcement during the final cutover.

## Issue one company licence

On a Kuiper-owned private computer with the existing encrypted Ed25519
signing key and private passphrase:

    python scripts/airvote_licence_issuer.py issue \
      --key /safe/airvote-private-encrypted.pem \
      --installation-id YOUR-INSTALLATION-UUID \
      --company existing-airvote \
      --customer "Broadcasting Company Name" \
      --plan enterprise --start 2026-11-17 --expires 2027-11-17 \
      --grace-days 7 --output broadcaster-company.lic

The same encrypted key and PUBLIC verification key are reused to avoid
interrupting existing licenced stations. The issuer no longer creates
individual station grants.

## Activate and renew

An authorised AirVote administrator can visit /licensing/activate/ and
paste the AV2 company licence. Alternatively install it on the server:

    python manage.py install_airvote_licence /secure/broadcaster-company.lic

The token covers every station owned by the licensed company. Renewal
requires only one newly issued company token. To revoke all company
access (without deleting historical votes):

    python manage.py revoke_airvote_licence existing-airvote \
      --reason "Agreement terminated"

The company cannot reinstate revoked tokens issued before revocation.
The one current CompanyLicence record overrides any transitional token.

## Environment

AIRVOTE_LICENCE_PUBLIC_KEY: public Ed25519 verification key, never private
AIRVOTE_LICENCE_INSTALLATION_ID: persistent installation UUID
AIRVOTE_COMPANY_BOOTSTRAP_LICENCES: optional JSON mapping company slug
  to one signed AV2 transitional company token
AIRVOTE_COMPANY_LICENSING_ENFORCED: true after provisioning and validation

Old AIRVOTE_BOOTSTRAP_LICENCES and AIRVOTE_LICENSING_ENFORCED are used only
for safe rollback to the previous AV1 implementation.

Never apply unrelated staged Railway changes during the cutover. Back up
and test the PostgreSQL migration first. Validate all existing stations,
a newly added station, revoked/expired company licence, unchanged voting
webhooks, presenter permissions and chart rankings.

This is per-company licensing within a hosted AirVote installation. It
does not yet manage online payments, computer seat counts, purchase orders
or remote licensor-hosted revocation. Commercial contracts and local
licensing law require professional review.
