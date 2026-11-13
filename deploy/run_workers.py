"""Supervise independent intake, matching and reply processes in one Railway service.

This creates independent queue consumers without replacing the proven PostgreSQL
job queue. Railway can later move each queue into its own service unchanged.
"""
import os
import signal
import subprocess
import sys
import time


def main():
    configured = os.getenv("AIRVOTE_WORKER_QUEUES", "intake,matching,replies")
    queues = [q.strip() for q in configured.split(",") if q.strip()]
    if not queues or len(set(queues)) != len(queues) or any(
        q not in {"intake", "matching", "replies"} for q in queues
    ):
        raise SystemExit("Invalid AIRVOTE_WORKER_QUEUES configuration")
    matching_workers = int(os.getenv("AIRVOTE_MATCHING_WORKERS", "1"))
    if not 1 <= matching_workers <= 4:
        raise SystemExit("AIRVOTE_MATCHING_WORKERS must be from 1 to 4")
    names = [q for q in queues for _ in range(matching_workers if q == "matching" else 1)]
    children = []

    def stop(signum=None, frame=None):
        for proc in children:
            if proc.poll() is None:
                proc.terminate()
        for proc in children:
            try:
                proc.wait(timeout=12)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        raise SystemExit(0 if signum is not None else 1)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for queue in names:
            children.append(subprocess.Popen(
                [sys.executable, "-u", "manage.py", "run_vote_worker", "--queue", queue],
                stdout=None, stderr=None,
            ))
        while True:
            if any(proc.poll() is not None for proc in children):
                stop()
            time.sleep(1)
    except BaseException:
        if any(proc.poll() is None for proc in children):
            for proc in children:
                if proc.poll() is None:
                    proc.terminate()
        raise


if __name__ == "__main__":
    main()
