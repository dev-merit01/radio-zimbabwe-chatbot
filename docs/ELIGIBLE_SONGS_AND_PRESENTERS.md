# AirVote presenter accounts and published eligible songs

## Permissions

- **Central administrator:** manages stations, programme configuration,
  presenter accounts, voting schedules and station-level catalogue verification.
  The administrator does **not** approve individual eligible song lists.
- **Presenter:** uses an individually assigned login for one chart programme.
  The station is derived from that programme. Server-side checks prevent
  editing lists belonging to another presenter, show or station.
- **Station accounts:** continue working as configured. An existing station
  user can be assigned to a specific show without changing its password.

## Prepare and publish each chart edition

1. The administrator assigns a presenter login to the programme.
2. The presenter opens **Eligible songs** and creates an edition for the
   upcoming broadcast date. The eligible candidate count may exceed the
   published Top 20, Top 40 or Top 50 chart size.
3. The presenter searches the station's **verified music catalogue** and
   adds, removes or replaces songs while the edition is a **Draft**.
4. The presenter clicks **Publish eligible list** directly. **There is no
   administrator approval step.** Publication activates eligibility for that
   programme and freezes the list for the edition.
5. When the programme's configured voting schedule opens within that edition,
   its display status becomes **Voting Open**. List publication is not itself
   a command to start or change the voting schedule.
6. The presenter prepares the next edition as a new draft.

The interface displays **Draft**, **Published** and **Voting Open**. Internally
the database retains the historical `locked` status value for published
editions so ranking queries and prior integrations remain compatible. An
upgrade migration returns previously `submitted` or `returned` editions to
editable drafts, preserving their selected candidate songs.

## Music verification is not approval of a shortlist

Presenters may propose music not yet in the verified catalogue. An administrator
may verify the authenticity of the new recording and link a proposal to a
catalogue song. A pending suggestion **does not block publication** of the
verified songs already chosen. Songs verified after an edition was published
may be considered for a future edition, but are never added retroactively to
a published eligible list.

## Voting integrity and historical preservation

- Bird continues to receive one WhatsApp number per station and configured
  chart-show voting codes, with no automatic listener replies.
- OpenAI matches incoming votes to verified catalogue songs. Only songs in
  the published eligible list for that exact station, programme and voting
  period count towards its official chart ranking.
- A vote for a song outside the published list is kept in the raw receipt
  data and may show as nomination interest. It is not transferred to the next
  edition or added to the official chart.
- The presenter **cannot publish** an edition after votes have been received
  for that edition's voting period or while that edition's voting window is
  currently open. This prevents retrospective song exclusions.
- Only verified songs from the programme's station may be published, and
  published eligible lists cannot overlap for a programme.
- Published lists are immutable; changes to a future edition require a new
  draft. Existing historical votes and archived results are not rewritten.
- Each publish action is recorded in the audit trail with the presenter and
  candidate count. Duplicate-vote and spam controls remain independent.

## Deployment

Run Django migrations before the updated application starts. There is no new
administrator approval workflow to configure. Test with a presenter login:
create a future edition, add verified songs, publish, confirm that the list
appears as **Published**, and check ranking eligibility. Also confirm that
publishing a list after voting starts is refused.
