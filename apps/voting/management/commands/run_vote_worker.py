import os
import socket
import time
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.utils import timezone
from apps.voting.models import InboundEvent, VoteJob, OutboundMessage, WorkerHeartbeat
from apps.voting.worker import run_one


class Command(BaseCommand):
    help = "Run the durable version 2 worker. Use one worker for SQLite development."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true")
        parser.add_argument(
            "--queue", choices=["all", "intake", "matching", "replies"], default="all"
        )
        parser.add_argument("--allow-sqlite", action="store_true")

    def handle(self, *args, **options):
        if connection.vendor != "postgresql" and not options["allow_sqlite"]:
            raise CommandError(
                "Production workers require PostgreSQL. Use --allow-sqlite for local single-worker development."
            )
        name = f"{socket.gethostname()}:{os.getpid()}"
        queues = {
            "intake": InboundEvent,
            "matching": VoteJob,
            "replies": OutboundMessage,
        }
        selected = (
            list(queues.values())
            if options["queue"] == "all"
            else [queues[options["queue"]]]
        )
        self.stdout.write("Version 2 worker running. Ctrl+C to stop.")
        try:
            while True:
                WorkerHeartbeat.objects.update_or_create(
                    name=name,
                    defaults={"last_seen": timezone.now(), "queue": options["queue"]},
                )
                work = False
                for model in selected:
                    for _ in range(1):
                        if not run_one(model):
                            break
                        work = True
                if options["once"]:
                    return
                if not work:
                    time.sleep(1)
        except KeyboardInterrupt:
            self.stdout.write("Worker stopped.")
