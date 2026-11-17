import {
  hmacHex, sha256Hex, signature, validSignedRequest, verifyGoogle,
  verifyTurnstile, localDay, safeUUID,
} from "./security.js";

/* AirVote public gateway: no secrets in the browser, no connection to
   PostgreSQL, and no user-controlled station/show/song admission. */
const json = (body, status = 200) => new Response(JSON.stringify(body), {
  status, headers: {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
  },
});
const error = (message, status = 400) => json({ error: message }, status);
const reject = () => error("Not authorised.", 401);
const RATE_WINDOW = 3600000;
const safeCode = (value) => typeof value === "string" && /^[A-Z0-9]{2,16}$/i.test(value);
const safeStation = (value) => typeof value === "string" && /^[a-z][a-z0-9_]{2,31}$/.test(value);
const safeCampaign = (value) => typeof value === "string" && /^[\w.:-]{10,800}$/.test(value);

function backendReady(env) {
  try {
    const url = new URL(env.BACKEND_URL);
    return url.protocol === "https:" && url.username === "" && url.password === ""
      && env.BRIDGE_READ_SECRET?.length >= 32;
  } catch { return false; }
}

async function backendRead(env, route) {
  if (!backendReady(env)) throw new Error("Read-only bridge unavailable.");
  const url = new URL("/api/voting-bridge/" + route, env.BACKEND_URL);
  const path = url.pathname + url.search;
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signed = await signature(env.BRIDGE_READ_SECRET, "GET", path, timestamp);
  const response = await fetch(url, {
    headers: { "X-AirVote-Timestamp": timestamp, "X-AirVote-Signature": signed },
    redirect: "error",
    signal: AbortSignal.timeout(6000),
  });
  if (!response.ok) throw new Error("The station is not available for public voting.");
  return response.json();
}

async function loadCampaign(env, token) {
  if (!safeCampaign(token)) throw new Error("Invalid voting link.");
  const data = await backendRead(env, "campaign/" + encodeURIComponent(token));
  if (!data?.station?.id || !Array.isArray(data.songs) || data.songs.length > 1000) {
    throw new Error("Invalid voting campaign.");
  }
  return data;
}

async function requestBody(request, max = 8192) {
  const length = Number(request.headers.get("Content-Length") || "0");
  if (length > max) throw new Error("Request too large.");
  const text = await request.text();
  if (text.length > max) throw new Error("Request too large.");
  const data = JSON.parse(text);
  if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error("Invalid request.");
  return data;
}

async function issueGoogleNonce(env) {
  if (!env.VOTER_HASH_KEY || env.VOTER_HASH_KEY.length < 32) return "";
  const stamp = Math.floor(Date.now() / 1000);
  const challenge = crypto.randomUUID() + "." + stamp;
  const mac = await hmacHex(env.VOTER_HASH_KEY, "google-nonce-v1|" + challenge);
  return challenge + "." + mac;
}

async function validGoogleNonce(env, nonce) {
  if (typeof nonce !== "string" ||
      !/^[a-f0-9-]{36}\\.\\d{10,11}\\.[a-f0-9]{64}$/.test(nonce)) return false;
  const parts = nonce.split(".");
  if (Math.abs(Math.floor(Date.now() / 1000) - Number(parts[1])) > 600) return false;
  const expected = await hmacHex(env.VOTER_HASH_KEY, "google-nonce-v1|" + parts[0] + "." + parts[1]);
  let diff = 0;
  for (let i = 0; i < 64; i++) diff |= expected.charCodeAt(i) ^ parts[2].charCodeAt(i);
  return diff === 0;
}

async function accountRateLimit(env, voterKey, maxAttempts = 30) {
  const windowStart = Math.floor(Date.now() / RATE_WINDOW);
  const result = await env.VOTES.prepare(
    "INSERT INTO request_limits (voter_key, window_start, attempts) VALUES (?, ?, 1) " +
    "ON CONFLICT(voter_key, window_start) DO UPDATE SET attempts = attempts + 1 RETURNING attempts",
  ).bind(voterKey, windowStart).first();
  return result && result.attempts <= maxAttempts;
}

async function preliminaryLimits(env, config, voterKey, day, songId) {
  // These cheap early rejects improve usability; the atomic PostgreSQL
  // station lock is authoritative, including concurrent and cross-channel votes.
  const station = config.station.id;
  const total = await env.VOTES.prepare(
    "SELECT COUNT(*) AS n FROM held_votes WHERE station = ? AND voter_key = ? " +
    "AND vote_day = ? AND state IN ('pending','processing','accepted')",
  ).bind(station, voterKey, day).first();
  if (config.station_daily_limit !== null && total.n >= config.station_daily_limit) {
    return "Daily voting limit reached for this station.";
  }
  const show = await env.VOTES.prepare(
    "SELECT COUNT(*) AS n FROM held_votes WHERE station = ? AND show_id = ? " +
    "AND voter_key = ? AND vote_day = ? AND state IN ('pending','processing','accepted')",
  ).bind(station, config.show_id, voterKey, day).first();
  if (show.n >= config.daily_limit) return "Daily voting limit reached for this programme.";
  if (!config.allow_repeat_song) {
    const params = [station, voterKey, day, songId];
    let query = "SELECT 1 AS found FROM held_votes WHERE station = ? AND voter_key = ? " +
      "AND vote_day = ? AND song_id = ? AND state IN ('pending','processing','accepted')";
    if (config.repeat_scope !== "station") {
      query += " AND show_id = ?";
      params.push(config.show_id);
    }
    if (await env.VOTES.prepare(query).bind(...params).first()) {
      return "You have already voted for this song today.";
    }
  }
  return null;
}

async function submitVote(request, env) {
  if (!env.GOOGLE_CLIENT_ID || !env.TURNSTILE_SECRET || !env.VOTER_HASH_KEY ||
      env.VOTER_HASH_KEY.length < 32 || !env.VOTES) return error("Voting is not configured.", 503);
  if (request.headers.get("Origin") !== new URL(request.url).origin) {
    return error("Please vote from the official voting website.", 403);
  }
  const remoteIp = request.headers.get("CF-Connecting-IP");
  if (typeof remoteIp !== "string" || !remoteIp || remoteIp.length > 64) {
    return error("Secure network verification unavailable.", 503);
  }
  // Hash the client IP with a changing day scope: no plaintext IP in storage.
  // This limits unauthenticated verification floods before Google or Turnstile.
  const ipKey = await hmacHex(env.VOTER_HASH_KEY,
    "rate-ip-v1|" + localDay(new Date()) + "|" + remoteIp);
  if (!(await accountRateLimit(env, "ip:" + ipKey, 90))) {
    return error("Too many requests from this network. Try again later.", 429);
  }
  let data;
  try { data = await requestBody(request); }
  catch { return error("Invalid submission."); }
  if (!safeCampaign(data.campaign) || !Number.isSafeInteger(data.song_id) ||
      data.song_id <= 0 || !safeUUID(data.request_id) ||
      typeof data.turnstile_token !== "string" ||
      typeof data.google_credential !== "string") {
    return error("Invalid submission.");
  }
  let account;
  try {
    account = await verifyGoogle(data.google_credential, env.GOOGLE_CLIENT_ID);
    if (account.nonce !== data.google_nonce ||
        !(await validGoogleNonce(env, data.google_nonce))) {
      throw new Error("Google sign-in challenge expired.");
    }
  } catch { return error("Please sign in with Google again.", 401); }
  let config;
  try { config = await loadCampaign(env, data.campaign); }
  catch { return error("Voting is temporarily unavailable.", 503); }
  if (!config.open || !config.songs.some(song => song.id === data.song_id)) {
    return error("Voting is closed or this song is not eligible.", 409);
  }
  const hostname = new URL(request.url).hostname;
  let human = false;
  try { human = await verifyTurnstile(
    data.turnstile_token, env.TURNSTILE_SECRET, hostname, request,
  ); } catch { /* Fail closed if the provider is unavailable. */ }
  if (!human) return error("Security verification expired. Please try again.", 403);
  const voterKey = await hmacHex(env.VOTER_HASH_KEY, "airvote-v1|" + config.station.id + "|" + account.sub);
  if (!(await accountRateLimit(env, voterKey))) return error("Too many attempts. Please try later.", 429);
  const day = localDay(new Date(), config.timezone || "Africa/Harare");
  const policyError = await preliminaryLimits(env, config, voterKey, day, data.song_id);
  if (policyError) return error(policyError, 409);

  const receiptSecretBytes = crypto.getRandomValues(new Uint8Array(32));
  const receiptSecret = [...receiptSecretBytes].map(b => b.toString(16).padStart(2, "0")).join("");
  const receiptHash = await sha256Hex(receiptSecret);
  const receivedAt = new Date().toISOString();
  try {
    await env.VOTES.prepare(
      "INSERT INTO held_votes (id, station, show_id, song_id, voter_key, vote_day, " +
      "received_at, state, receipt_hash, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
    ).bind(
      data.request_id, config.station.id, config.show_id, data.song_id, voterKey,
      day, receivedAt, receiptHash, Date.now(),
    ).run();
  } catch {
    // A client retry with the same request ID cannot add a second vote.
    return error("This submission was already received. Check your receipt.", 409);
  }
  return json({
    receipt_id: data.request_id,
    receipt_secret: receiptSecret,
    state: "pending",
    message: "Vote received securely. Awaiting AirVote verification.",
  }, 202);
}

async function receipt(request, env, id) {
  if (!safeUUID(id)) return error("Receipt not found.", 404);
  const bearer = request.headers.get("Authorization") || "";
  if (!/^Bearer [a-f0-9]{64}$/.test(bearer)) return reject();
  const secretHash = await sha256Hex(bearer.slice(7));
  const row = await env.VOTES.prepare(
    "SELECT state, decision FROM held_votes WHERE id = ? AND receipt_hash = ?",
  ).bind(id, secretHash).first();
  if (!row) return error("Receipt not found or expired.", 404);
  return json({
    state: row.state,
    message: row.state === "accepted" ? "Submitted to AirVote for final verification." :
      row.state === "rejected" ? "This vote could not be accepted." :
      "Your submission is awaiting verification.",
  });
}

async function claim(request, env) {
  let data;
  try { data = await requestBody(request, 2048); } catch { return error("Invalid request."); }
  const limit = Number.isInteger(data.limit) ? Math.min(20, Math.max(1, data.limit)) : 10;
  const now = Date.now();
  const lease = crypto.randomUUID();
  const rows = await env.VOTES.prepare(
    "UPDATE held_votes SET state = 'processing', lease_until = ?, lease_token = ?, " +
    "attempts = attempts + 1, updated_at = ? WHERE id IN (" +
    "SELECT id FROM held_votes WHERE (state = 'pending' OR " +
    "(state = 'processing' AND lease_until < ?)) AND attempts < 8 " +
    "ORDER BY received_at LIMIT ?) " +
    "RETURNING id, station, show_id, song_id, voter_key, received_at, lease_token",
  ).bind(now + 120000, lease, now, now, limit).all();
  return json({ votes: rows.results || [] });
}

async function ack(request, env) {
  let data;
  try { data = await requestBody(request, 10000); } catch { return error("Invalid request."); }
  if (!Array.isArray(data.items) || data.items.length > 20) return error("Invalid acknowledgements.");
  for (const item of data.items) {
    if (!safeUUID(item.id) || !safeUUID(item.lease_token) ||
        !["accepted", "rejected"].includes(item.state) ||
        typeof item.reason !== "string" || !/^[a-z_]{0,64}$/.test(item.reason)) {
      return error("Invalid acknowledgement.");
    }
  }
  for (const item of data.items) {
    await env.VOTES.prepare(
      "UPDATE held_votes SET state = ?, decision = ?, updated_at = ?, " +
      "lease_until = NULL, lease_token = NULL " +
      "WHERE id = ? AND lease_token = ? AND state = 'processing'",
    ).bind(item.state, item.reason, Date.now(), item.id, item.lease_token).run();
  }
  return json({ ok: true });
}

const securityHeaders = {
  "Content-Security-Policy": "default-src 'none'; script-src 'self' https://accounts.google.com " +
    "https://challenges.cloudflare.com; style-src 'self' 'unsafe-inline' https://accounts.google.com " +
    "https://challenges.cloudflare.com; img-src 'self' https: data:; font-src 'self'; " +
    "frame-src https://accounts.google.com https://challenges.cloudflare.com; " +
    "connect-src 'self' https://accounts.google.com https://challenges.cloudflare.com; " +
    "base-uri 'none'; form-action 'self'; frame-ancestors 'none'; upgrade-insecure-requests",
  "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
};

async function route(request, env) {
  const url = new URL(request.url);
  const { pathname, searchParams } = url;
  if (request.method === "GET" && pathname === "/api/site") return json({
    google_client_id: env.GOOGLE_CLIENT_ID || "",
    google_nonce: await issueGoogleNonce(env),
    turnstile_site_key: env.TURNSTILE_SITE_KEY || "",
    ready: Boolean(backendReady(env) && env.GOOGLE_CLIENT_ID && env.TURNSTILE_SECRET &&
      env.VOTER_HASH_KEY?.length >= 32 && env.VOTES),
  });
  if (request.method === "GET" && pathname === "/api/stations") {
    try { return json(await backendRead(env, "stations")); }
    catch { return error("Station directory is temporarily unavailable.", 503); }
  }
  if (request.method === "GET" && pathname === "/api/resolve") {
    const station = searchParams.get("station");
    const code = searchParams.get("code");
    if (!safeStation(station) || !safeCode(code)) return error("Invalid station or voting code.");
    try {
      return json(await backendRead(env, "resolve/" + station + "/" + code.toUpperCase()));
    } catch { return error("No open programme uses this code.", 404); }
  }
  if (request.method === "GET" && pathname.startsWith("/api/campaign/")) {
    try { return json(await loadCampaign(env, pathname.slice("/api/campaign/".length))); }
    catch { return error("Voting link unavailable.", 404); }
  }
  if (request.method === "POST" && pathname === "/api/vote") return submitVote(request, env);
  if (request.method === "GET" && pathname.startsWith("/api/receipt/")) {
    return receipt(request, env, pathname.slice("/api/receipt/".length));
  }
  if (pathname.startsWith("/internal/")) {
    if (request.method !== "POST" || !(await validSignedRequest(request, env.PULL_SECRET))) {
      return reject();
    }
    if (pathname === "/internal/claim") return claim(request, env);
    if (pathname === "/internal/ack") return ack(request, env);
    return error("Not found.", 404);
  }
  if (request.method !== "GET" && request.method !== "HEAD") return error("Method not allowed.", 405);
  if (pathname === "/" || pathname === "/index.html" || pathname.startsWith("/vote/")) {
    const pageUrl = new URL("/index.html", url);
    const page = await env.ASSETS.fetch(new Request(pageUrl, request));
    const headers = new Headers(page.headers);
    for (const [key, value] of Object.entries(securityHeaders)) headers.set(key, value);
    headers.set("Cache-Control", "no-store");
    return new Response(page.body, { status: page.status, headers });
  }
  return env.ASSETS.fetch(request);
}

export default {
  async fetch(request, env) {
    try { return await route(request, env); }
    catch { return error("Service temporarily unavailable.", 503); }
  },
  async scheduled(_event, env) {
    const now = Date.now();
    // Never delete still-unacknowledged receipts before the 72-hour retry window.
    await env.VOTES.prepare(
      "UPDATE held_votes SET state = 'rejected', decision = 'expired', " +
      "updated_at = ? WHERE state IN ('pending','processing') AND received_at < ?",
    ).bind(now, new Date(now - 72 * 3600000).toISOString()).run();
    await env.VOTES.prepare(
      "DELETE FROM held_votes WHERE state IN ('accepted','rejected') AND updated_at < ?",
    ).bind(now - 24 * 3600000).run();
    await env.VOTES.prepare("DELETE FROM request_limits WHERE window_start < ?")
      .bind(Math.floor(now / RATE_WINDOW) - 24).run();
  },
};
