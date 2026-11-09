# AirVote presenter accounts & station-approved eligible songs

## Roles

- **Central administrator:** keeps the existing superuser login; chooses any of the
  seven stations and any chart show. This account is not assigned to one show.
- **Presenter:** has a separate username/password and a mandatory, one-to-one
  assignment to one chart show. The station is derived from that show. Station
  switching, another show's API ID, another show's archives and station-wide
  administration are forbidden on the server, not just hidden in the interface.
- **Existing station accounts:** continue working as station-team accounts.
  The administrator can assign an existing account to a specific show using
  **Presenter accounts**; this does not reveal or reset its password.

## Prepare each edition before voting

1. Central administrator signs in, chooses the station, and opens
   **Presenter accounts**. Create a unique presenter account for the specific
   chart show, or assign an existing account. Send their password privately;
   do not publish it with station voting codes.
2. Presenter signs in; their station and chart show are selected automatically.
   No selection or station password is required.
3. Open **Eligible songs**, create an edition with its scheduled broadcast
   date (e.g. Saturday Top 20), search the verified station catalogue, and add
   the chosen songs to a draft shortlist.
4. Missing music can be proposed with its artist and title. The administrator
   first verifies the real recording in the station's catalogue, then links
   the suggestion to the verified song. Artist fame or compulsory airplay is
   **not** a qualification criterion; song authenticity, programme suitability,
   and broadcast availability are reviewed fairly.
5. Presenter submits the list. Administrator can return it with a reason, or
   approve and lock it **before voting opens**. Pending song suggestions must
   be resolved before approval. The list cannot be edited once locked.
6. When the first approved edition is locked, the programme's
   `eligibility_enforced` flag turns on. It applies only to that programme.
   Existing historical station votes and programme results are unaffected
   until that programme's first approval. Every future edition must have an
   approved shortlist for songs to count toward its official rankings.

## Ranking and nominations

- Bird keeps using **one WhatsApp number per station** and programme-specific
  voting codes. There are **no automatic replies**.
- OpenAI verifies incoming music identification as before, but the official
  Top 20/50 ranking additionally filters to the **administrator-locked edition
  candidate IDs**. This separates song *verification* from chart *eligibility*.
- A vote for a song outside the shortlist is preserved in the raw receipt/audit
  data and displayed as **nomination interest**, not counted in the official
  chart. It is **never carried into another edition**.
- Legitimate voting for an approved emerging artist may still put that song
  at number 1. Fame or airplay is not used to artificially rearrange votes.
- Rankings and official archives retain their original station/show scoping
  and duplicate checks. Existing spam and vote-multiplier policies remain
  unchanged.
- The admin cannot approve a shortlist after that edition has received votes
  or started voting. Once editions are locked, changes to programme chart size,
  broadcast day or schedule are restricted to protect historical comparability.

## Safe initial deployment

Both migrations add new schema only; no existing accounts are changed, no
previous votes are reassigned, and no current programme becomes restricted
automatically. The administrator must **create or assign presenters and approve
the first future shortlist** to activate eligibility for each show.

Before announcing programme voting, test one presenter sign-in, one approved
song, one out-of-list nomination and the end-of-year campaign under the
selected station. Only then activate code-based voting for the station.

## Limitations

This release provides individual account assignment, verified-catalogue
search, dated shortlists, new-song proposals, administrator verification/
approval and locked-chart filtering. Bulk Excel importing and advanced
programme-specific audio-quality checking remain later enhancements.
