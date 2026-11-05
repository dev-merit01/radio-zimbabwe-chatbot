#!/bin/sh
set -eu
python deploy/check_environment.py
# Never serve updated models against an older database schema.
# This also protects deployments where the platform pre-deploy hook is skipped.
python manage.py migrate --noinput
# Audited one-time catalogue batch; subsequent starts preserve all decisions.
python manage.py import_verified_catalogue --all-stations
# Accounts are seeded only when a one-off private Railway variable is supplied.
# Never keep passwords in GitHub or log the environment variable.
if [ -n "${AIRVOTE_STATION_ACCOUNTS_JSON:-}" ]; then
    python manage.py provision_station_accounts
fi
exec gunicorn radio_zimbabwe.wsgi:application --bind "0.0.0.0:${PORT:-8000}" --workers "${WEB_CONCURRENCY:-1}" --threads "${WEB_THREADS:-4}" --timeout 60 --access-logfile - --error-logfile -
