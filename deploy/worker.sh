#!/bin/sh
set -eu
python deploy/check_environment.py
exec python -u deploy/run_workers.py
