/* Small, auditable primitives shared by the Workers security gateway.
 * No third-party JWT library or long-lived external identity token storage.
 */
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function hex(bytes) {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function base64urlBytes(value) {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]+$/.test(value) || value.length > 10000) {
    throw new Error("Invalid base64url.");
  }
  const padded = value.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - value.length % 4) % 4);
  return Uint8Array.from(atob(padded), (char) => char.charCodeAt(0));
}

export async function sha256Hex(input) {
  const data = typeof input === "string" ? encoder.encode(input) : input;
  return hex(new Uint8Array(await crypto.subtle.digest("SHA-256", data)));
}

export async function hmacHex(secret, value) {
  if (typeof secret !== "string" || secret.length < 32) {
    throw new Error("Gateway secret not configured.");
  }
  const key = await crypto.subtle.importKey(
    "raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"],
  );
  return hex(new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(value))));
}

export async function signature(secret, method, path, timestamp, body = "") {
  const digest = await sha256Hex(body);
  return hmacHex(secret, [timestamp, method.toUpperCase(), path, digest].join("\n"));
}

export function localDay(date = new Date(), timeZone = "Africa/Harare") {
  const pieces = Object.fromEntries(new Intl.DateTimeFormat("en-GB", {
    timeZone, year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(date).filter((part) => part.type !== "literal").map((part) => [part.type, part.value]));
  return [pieces.year, pieces.month, pieces.day].join("-");
}

export function safeUUID(value) {
  return typeof value === "string"
    && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

export async function validSignedRequest(request, secret) {
  if (typeof secret !== "string" || secret.length < 32) return false;
  const time = request.headers.get("X-AirVote-Timestamp") || "";
  const received = request.headers.get("X-AirVote-Signature") || "";
  if (!/^\d{10,11}$/.test(time) || !/^[0-9a-f]{64}$/.test(received)) return false;
  if (Math.abs(Date.now() / 1000 - Number(time)) > 60) return false;
  const url = new URL(request.url);
  const path = url.pathname + url.search;
  const body = await request.clone().text();
  const expected = await signature(secret, request.method, path, time, body);
  // Avoid branch-on-first-mismatch; an HMAC is never accepted on a partial match.
  let diff = 0;
  for (let i = 0; i < 64; i++) diff |= received.charCodeAt(i) ^ expected.charCodeAt(i);
  return diff === 0;
}

let googleKeys = null;
let googleKeysUntil = 0;

export async function verifyGoogle(token, clientId) {
  if (!clientId || typeof token !== "string" || token.length > 8192) throw new Error("Google verification unavailable.");
  const parts = token.split(".");
  if (parts.length !== 3 || parts.some((part) => !part)) throw new Error("Invalid Google credential.");
  const header = JSON.parse(decoder.decode(base64urlBytes(parts[0])));
  const claims = JSON.parse(decoder.decode(base64urlBytes(parts[1])));
  if (header.alg !== "RS256" || typeof header.kid !== "string") throw new Error("Unsupported Google signature.");
  const now = Math.floor(Date.now() / 1000);
  if (claims.iss !== "https://accounts.google.com" && claims.iss !== "accounts.google.com") {
    throw new Error("Unexpected Google issuer.");
  }
  if (claims.aud !== clientId || !Number.isInteger(claims.exp) || claims.exp <= now
    || !Number.isInteger(claims.iat) || claims.iat > now + 60
    || typeof claims.sub !== "string" || !/^\d{5,40}$/.test(claims.sub)
    || claims.email_verified !== true) {
    throw new Error("Google account could not be verified.");
  }
  if (!googleKeys || Date.now() > googleKeysUntil) {
    const response = await fetch("https://www.googleapis.com/oauth2/v3/certs", {
      headers: { Accept: "application/json" },
    });
    if (!response.ok) throw new Error("Google key service unavailable.");
    const data = await response.json();
    if (!Array.isArray(data.keys)) throw new Error("Invalid Google keys.");
    googleKeys = data.keys;
    googleKeysUntil = Date.now() + 15 * 60 * 1000;
  }
  let jwk = googleKeys.find((key) => key.kid === header.kid && key.kty === "RSA" && key.use === "sig");
  if (!jwk) {
    googleKeys = null; // rotated Google keys will be fetched on the next request.
    throw new Error("Google signing key unavailable.");
  }
  const key = await crypto.subtle.importKey(
    "jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"],
  );
  const signed = encoder.encode(parts[0] + "." + parts[1]);
  const verified = await crypto.subtle.verify(
    "RSASSA-PKCS1-v1_5", key, base64urlBytes(parts[2]), signed,
  );
  if (!verified) throw new Error("Invalid Google signature.");
  return { sub: claims.sub, nonce: claims.nonce || "" };
}

export async function verifyTurnstile(token, secret, expectedHostname, request) {
  if (!secret || typeof token !== "string" || !token || token.length > 2048) return false;
  const response = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      secret, response: token, remoteip: request.headers.get("CF-Connecting-IP") || undefined,
    }),
  });
  if (!response.ok) return false;
  const result = await response.json();
  return result.success === true && result.hostname === expectedHostname && result.action === "vote";
}
