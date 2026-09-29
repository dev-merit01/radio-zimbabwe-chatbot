#!/bin/sh
set -eu
python deploy/check_environment.py
exec gunicorn radio_zimbabwe.wsgi:application --bind "0.0.0.0:${PORT:-8000}" --workers "${WEB_CONCURRENCY:-1}" --threads "${WEB_THREADS:-4}" --timeout 60 --access-logfile - --error-logfile -
