#!/usr/bin/env python3
"""Kuiper Data Labs offline Ed25519 issuer. Keep the private key OFF Railway.

Examples:
    python scripts/airvote_licence_issuer.py create-keys --directory ./private-issuer
    python scripts/airvote_licence_issuer.py issue --key ./private-issuer/airvote-private.pem \
      --station national_fm --customer "Example Broadcaster" --plan annual \
      --expires 2027-11-17 --output ./national-fm.lic

The issuer never transmits licences or stores its private key on the server.
"""
import argparse
import base64
from datetime import date, datetime, timezone
import getpass
import json
import os
from pathlib import Path
import re
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

PLANS = ("trial", "annual", "enterprise", "perpetual")
STATION = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def protected_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(data)


def create_keys(args):
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    password = getpass.getpass("New signing-key password (at least 12 characters): ")
    confirmation = getpass.getpass("Confirm password: ")
    if len(password) < 12 or password != confirmation:
        raise SystemExit("Passwords must match and contain at least 12 characters.")
    private = Ed25519PrivateKey.generate()
    secret = private.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(password.encode()),
    )
    public = private.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw,
    )
    protected_write(directory / "airvote-private.pem", secret)
    protected_write(directory / "airvote-public.txt", base64.b64encode(public) + b"\n")
    print("Created encrypted private signing key and public verification key.")
    print("Back up the PRIVATE key securely. Never upload it to GitHub or Railway.")
    print("Set AIRVOTE_LICENCE_PUBLIC_KEY to the text in airvote-public.txt on Railway.")


def issue(args):
    if not STATION.fullmatch(args.station):
        raise SystemExit("Station must be an AirVote slug (1–32 lowercase characters).")
    if not 1 <= len(args.customer.strip()) <= 160:
        raise SystemExit("Customer name must be between 1 and 160 characters.")
    try:
        begins = date.fromisoformat(args.start or date.today().isoformat())
        ends = date.fromisoformat(args.expires)
    except ValueError as exc:
        raise SystemExit("Use ISO dates YYYY-MM-DD.") from exc
    if ends < begins:
        raise SystemExit("Expiry cannot precede the start date.")
    password = getpass.getpass("Signing-key password: ")
    private = serialization.load_pem_private_key(
        Path(args.key).read_bytes(), password=password.encode()
    )
    if not isinstance(private, Ed25519PrivateKey):
        raise SystemExit("Expected an Ed25519 private key.")
    payload = {
        "issuer": "kuiperdatalabs.co.zw", "audience": "airvote",
        "licence_id": str(uuid.uuid4()), "station": args.station,
        "customer": args.customer.strip(), "plan": args.plan,
        "starts_on": begins.isoformat(), "expires_on": ends.isoformat(),
        "grace_days": args.grace_days,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    token = f"AV1.{b64(raw)}.{b64(private.sign(raw))}"
    protected_write(args.output, (token + "\n").encode("ascii"))
    print(f"Issued {args.plan} licence {payload['licence_id']} for {args.station}.")
    print(f"Customer: {payload['customer']}; start: {begins}; expires: {ends}.")
    print(f"Signed licence saved in {args.output}. Do not email the private signing key.")


def main():
    parser = argparse.ArgumentParser(description="Kuiper Data Labs AirVote licence issuer")
    sub = parser.add_subparsers(dest="action", required=True)
    keys = sub.add_parser("create-keys")
    keys.add_argument("--directory", required=True)
    keys.set_defaults(run=create_keys)
    licence = sub.add_parser("issue")
    licence.add_argument("--key", required=True)
    licence.add_argument("--station", required=True)
    licence.add_argument("--customer", required=True)
    licence.add_argument("--plan", required=True, choices=PLANS)
    licence.add_argument("--start")
    licence.add_argument("--expires", required=True)
    licence.add_argument("--grace-days", type=int, choices=range(0, 8), default=3)
    licence.add_argument("--output", required=True)
    licence.set_defaults(run=issue)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
