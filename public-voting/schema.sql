-- Isolated temporary votes: no raw Google tokens, emails, names or IPs.
-- The main AirVote database is not reachable by this Worker.
CREATE TABLE IF NOT EXISTS held_votes (
  id TEXT PRIMARY KEY NOT NULL,
  station TEXT NOT NULL,
  show_id INTEGER NOT NULL,
  song_id INTEGER NOT NULL,
  voter_key TEXT NOT NULL,
  vote_day TEXT NOT NULL,
  received_at TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'processing', 'accepted', 'rejected')),
  receipt_hash TEXT NOT NULL,
  lease_token TEXT,
  lease_until INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  decision TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_held_claim
  ON held_votes (state, lease_until, received_at);
CREATE INDEX IF NOT EXISTS idx_held_voter
  ON held_votes (station, voter_key, vote_day, state);
CREATE INDEX IF NOT EXISTS idx_held_cleanup
  ON held_votes (state, updated_at);
CREATE TABLE IF NOT EXISTS request_limits (
  voter_key TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  attempts INTEGER NOT NULL,
  PRIMARY KEY (voter_key, window_start)
);
