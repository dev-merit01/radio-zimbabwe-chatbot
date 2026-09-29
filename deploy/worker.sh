#!/bin/sh
set -eu
python deploy/check_environment.py
exec python manage.py run_vote_worker
