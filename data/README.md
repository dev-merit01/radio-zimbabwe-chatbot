# Zimbabwe 2026 starter catalogue

305 sourced tracks researched through 2026-10-30, including album tracks, EPs, singles and collaborations. This is not an exhaustive national discography. Dates describe the listed 2026 release or publication; the first release of every recording is not established. Every row retains its source, evidence class and review notes.

## Import

```sh
python manage.py import_catalogue data/zimbabwe_2026.json --all-stations --dry-run
python manage.py import_catalogue data/zimbabwe_2026.json --all-stations
```

Use `--station power_fm` instead of `--all-stations` to target one station. The six stations come from `Station.values`. New entries are **pending**, matching the existing seeding policy. Verify songs through the existing review workflow before automatic matching. Existing songs and decisions remain unchanged; the command does not alter votes or mappings.

To add songs later, append objects to the JSON or supply another schema_version 1 file. Required song fields: artist and title. Optional fields: featured_artists (a list of strings) and album. Other research metadata remains in the versioned JSON. Keep remix/live/acoustic distinctions in the title. Repeated artist/title rows in a file are rejected; disambiguate recording versions. Existing rows are skipped by normalized artist/title or full credit. Use the existing review/edit workflow for corrections; re-import does not overwrite metadata.

The importer validates the entire file before writing, locks stations in stable order, writes selected stations atomically, and logs counts plus the file SHA256. Dry run performs no writes and is an estimate if other imports occur concurrently. Main artist stays in artist and full credit stays in canonical_name, preserving main-artist vote matching without schema changes. The separate featured list remains in the source JSON, not a new database column. Existing dashboard editing may regenerate canonical_name; retain full credits when reviewing edits.

## Research limitations

- Album tracks may predate their 2026 album. release_year does not establish a recording's first publication year.
- Reggae Dzemahwindi, Love Freakquency and Rebound source track counts differ from enumerated tracks.
- Nkwali feature credits, Mark Ngwazi original release dates and some Mathias Mhere spellings need confirmation.
- Known earlier recordings Too Much (solo), Sengihamba, Angeke Kulunge and Ekhaya were omitted.
- Secondary listings require editorial checks. No audio is included.

## Validation

Six focused Django/SQLite tests cover all stations, repeat imports, preserving rejection, dry runs, malformed input, duplicate input, later additions, full credits and station-isolated main-artist matching after verification. Executed against fetched repository model/pipeline code with minimal local settings. Full application tests and PostgreSQL concurrency tests were not run locally.

```sh
python manage.py test apps.voting.tests.test_import_catalogue
```
