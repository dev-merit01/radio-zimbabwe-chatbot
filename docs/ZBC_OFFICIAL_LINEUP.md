# Official ZBC chart-show lineup — 9 November 2026

Source: **ZBC_Chart_Shows_Visual_Enhanced.docx**, supplied by the station team.
Each row is a separate AirVote chart programme with a permanent database ID,
its own editable WhatsApp code, presenter metadata and published chart archives.
The document lists **18 programmes across six stations**. Luzibo Radio is not
listed: its previous programme configuration is preserved without alteration.

## Broadcast programmes and short initial voting codes

The times below are **on-air broadcast times**, not WhatsApp voting hours.

| Station | Official chart show | Broadcast | Presenter as supplied | Suggested code | Positions |
| --- | --- | --- | --- | --- | ---: |
| Radio Zimbabwe | Gospel 20 | Saturday 08:00–09:00 | Thulani Siziba | G20 | 20 |
| Radio Zimbabwe | Glow Petroleum Top 20 | Saturday 12:00–13:00 | Not specified | GP20 | 20 |
| Radio Zimbabwe | Top 50 | Annual: voting 1–31 Dec 2026 | Not specified | Y50 | 50 |
| National FM | National FM Top 20 | Saturday 11:00–12:00 | Soneni | N20 | 20 |
| National FM | National FM Gospel 10 | Sunday 10:00–11:00 | Rachel | NG10 | 10 |
| National FM | National FM Top 50 | Annual: voting 1–31 Dec 2026 | Soneni | NY50 | 50 |
| Classic 263 | Gospel 20 | Sunday 19:00–20:00 | Rumbidzai Matinanga | CG20 | 20 |
| Classic 263 | Traffic Weekend Chart Show Top 20 | Saturday 12:00–13:00 | Obert Machiridza | CT20 | 20 |
| Classic 263 | Gospel End-of-Year Top 50 | End of year, time TBC | Not specified | CG50 | 50 |
| Classic 263 | Traffic Weekend Chart Show Top 50 | End of year, time TBC | Not specified | CT50 | 50 |
| Power FM | Power FM Top 40 | Saturday 06:00–10:00 | Promethius | P40 | 40 |
| Power FM | Gospel Top 20 | Sunday 18:00–21:00 | Rachel | PG20 | 20 |
| Power FM | Power FM Top 50 | End of year, time TBC | Not specified | P50 | 50 |
| Power FM | Power FM Gospel 50 | End of year, time TBC | Not specified | PG50 | 50 |
| Khulumani FM | Khulumani FM Top 20 | Sunday 12:00–13:00 | Mphumelelo Mandimande | K20 | 20 |
| Khulumani FM | Magnificent 10 – Hip-hop | Saturday 08:00–09:00 | Thorne Leraq | KH10 | 10 |
| Khulumani FM | Gospel 10 | Sunday 19:00; end time not supplied | Sandisiwe | KG10 | 10 |
| Central Radio | Central Radio Top 20 Chart Show | Saturday 08:00–12:00 | Madeleine Runyambo | C20 | 20 |

Codes are unique **within their station**, not across the full network.
If a proposed code is already in use by another programme on the same station,
the importer appends a short numeric suffix rather than reassigning votes.

## Listener instructions

The station has **one WhatsApp number**. Each programme has a separate code:
`G20 Artist - Song`, `GP20 Artist - Song` and `Y50 Artist - Song`.
The Bird receiving number identifies the station. The leading programme code
identifies its chart show. The existing OpenAI verifier matches the song; its
approved shortlist determines chart eligibility. No outbound WhatsApp replies
are sent.

## Editing and activation

**Admin control → select station → Chart shows → Edit settings**:
- Programme name, presenter and on-air start/end are editable.
- Voting code is editable (2–16 letters/digits, automatically uppercase).
  Changing it while voting is open requires explicit confirmation. Received
  votes still retain the original permanent programme ID.
- Chart size supports **Top 10, Top 20, Top 40 or Top 50**. Year-end campaigns
  must use Top 50. Published chart sizes are immutable.
- Voting schedule is separate: set repeating weekly hours, manual open/close
  or a fixed campaign date range, and configure the daily vote limit.
- Presenter username assignment and shortlist approval are separately managed
  in **Presenter accounts** and **Eligible songs**; a name in the ZBC timetable
  does not create credentials or bypass administrator approval.

**All newly configured and repurposed sample shows start disabled.** The
administrator must review their show names/codes, schedule voting windows,
approve eligible song lists for future editions, enable the shows and announce
the codes. Only then enable **Require programme codes** at station level.

Radio Zimbabwe and National FM explicitly provide 1–31 December 2026
**voting** periods. Both are prefilled from **00:00 on 1 December 2026 to the
exclusive start of 1 January 2027**, Zimbabwe local time. The source does not
specify campaign dates for Classic 263 and Power FM annual shows, so those
remain blank and disabled. End-of-year templates already present for other
stations remain disabled pending confirmed schedules.

## Non-destructive data migration

Migration `0020_zbc_official_chart_shows` reuses generic disabled templates
**only** if untouched, unarchived and unused by raw votes, inbound receipts,
published charts, approved lists, presenter assignments, unassigned cases
or spam records. Any used or customised show retains its ID, code, settings
and records. If necessary, the official programme is created separately
rather than replacing historical identity. The migration is idempotent and
does not enable voting, change Bird numbers, generate presenter credentials,
or change verified song rankings.
