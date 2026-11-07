# AirVote suspected-spam review

The seven stations share one voting backend and individual Bird numbers.
Spam checks apply to the **station identified by the receiving number**,
not a listener-provided station name, and never combine different senders.

## Incoming checks (before raw vote creation or OpenAI calls)

- **Repeated identical submissions:** the third equivalent, case/whitespace-
  normalised vote from the same sender, station and provider within 120 seconds
  is flagged. Earlier messages are still subject to existing daily and
  duplicate-song limits.
- **Rapid bursts:** starting with the sixth incoming vote from the same sender,
  station and provider in 60 seconds, the message is flagged, even if its song
  title differs.
- **Independent listeners:** a popular song receiving many votes from
  different phone numbers is *not* itself suspicious. No cross-listener
  automated sanctions or blocking are applied.
- **Bird retries:** repeat provider message IDs never create additional votes
  or override a previous flag.

Checks use the original authenticated webhook receipt time, not when a worker
eventually processes a message. They run under the existing per-station DB lock
to prevent concurrent messages bypassing a threshold.

## Administrator review

Open **Chart shows → Suspected spam votes** under the selected station.
The central administrator sees the masked listener reference, show name,
reason, original song text and receipt time. Only that administrator can:

- **Release:** return a held submission to its original station/chart show and
  receipt time. Normal daily limits, duplicate checks and OpenAI matching still
  apply; release is *not* an automatic vote approval.
- **Dismiss:** keep the suspicious submission in the audit database but never
  send it to vote counting or OpenAI.

Both decisions create a station audit event. Never release a message into a
period with an already-finalised chart. Presenter accounts have no access to
spam review details, and no automatic WhatsApp acknowledgements are sent.

## Interpretation

A flag indicates **suspected** spam, not proof of misconduct. Legitimate
listeners may send rapid votes; administrators can release false positives.
The system deliberately avoids blocking phone numbers or cancelling
previously accepted votes automatically. Existing chart totals and published
archives are not rewritten.
