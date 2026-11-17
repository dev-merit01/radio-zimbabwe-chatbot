import test from "node:test";
import assert from "node:assert/strict";
import { createHmac, webcrypto } from "node:crypto";
import {
  hmacHex, signature, validSignedRequest, localDay, safeUUID,
} from "../src/security.js";

if (!globalThis.crypto) globalThis.crypto = webcrypto;

test("shared HMAC format exactly matches Python's signing_message", async () => {
  const key = "a-private-shared-secret-with-adequate-length";
  const path = "/api/voting-bridge/stations";
  const timestamp = String(Math.floor(Date.now() / 1000));
  const canonical = [timestamp, "GET", path,
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"].join("\n");
  const expected = createHmac("sha256", key).update(canonical).digest("hex");
  assert.equal(await signature(key, "GET", path, timestamp), expected);
  const request = new Request("https://airvote.example.org" + path, {
    headers: { "X-AirVote-Timestamp": timestamp, "X-AirVote-Signature": expected },
  });
  assert.equal(await validSignedRequest(request, key), true);
  assert.equal(await validSignedRequest(
    new Request("https://airvote.example.org/other", { headers: request.headers }), key,
  ), false);
});

test("bad, expired and missing signatures are rejected", async () => {
  const key = "a-private-shared-secret-with-adequate-length";
  assert.equal(await validSignedRequest(new Request("https://example.org/internal/claim"), key), false);
  const stale = String(Math.floor(Date.now() / 1000) - 200);
  assert.equal(await validSignedRequest(new Request("https://example.org/internal/claim", {
    headers: {
      "X-AirVote-Timestamp": stale,
      "X-AirVote-Signature": await signature(key, "GET", "/internal/claim", stale),
    },
  }), key), false);
});

test("voter hashes are stable but station-scoped", async () => {
  const key = "this-is-a-test-only-hmac-secret-of-adequate-length";
  const one = await hmacHex(key, "airvote-v1|national_fm|1234567890");
  const same = await hmacHex(key, "airvote-v1|national_fm|1234567890");
  const another = await hmacHex(key, "airvote-v1|power_fm|1234567890");
  assert.equal(one, same);
  assert.notEqual(one, another);
  assert.match(one, /^[a-f0-9]{64}$/);
});

test("Zimbabwe voting dates stay local across UTC midnight", () => {
  assert.equal(localDay(new Date("2026-11-17T22:30:00Z")), "2026-11-18");
  assert.equal(localDay(new Date("2026-11-17T20:30:00Z")), "2026-11-17");
  assert.equal(safeUUID("e90b95c2-2d1f-4a9c-b349-97a70832b36e"), true);
  assert.equal(safeUUID("e90b95c2-2d1f-4a9c-b349-97a70832b36f"), true);
  assert.equal(safeUUID("not-a-vote-id"), false);
});
